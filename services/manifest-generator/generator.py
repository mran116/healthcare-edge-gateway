#!/usr/bin/env python3
"""
DICOM Manifest System for Series-Bundled Storage
Enables efficient querying and retrieval of bundled DICOM files
"""

import json
import tarfile
import io
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import boto3
import pydicom
from pathlib import Path

@dataclass
class InstanceInfo:
    """Information about a single DICOM instance"""
    instance_uid: str
    instance_number: int
    sop_class_uid: str
    file_size: int
    pixel_data_offset: int  # Offset within the TAR file
    pixel_data_size: int
    # Key DICOM tags for quick access
    window_center: Optional[float] = None
    window_width: Optional[float] = None
    rows: Optional[int] = None
    columns: Optional[int] = None
    bits_allocated: Optional[int] = None
    photometric_interpretation: Optional[str] = None
    # Position in bundle
    tar_offset: int = 0  # Byte offset in TAR file
    tar_size: int = 0    # Size in TAR file

@dataclass
class SeriesInfo:
    """Information about a DICOM series"""
    series_uid: str
    series_number: int
    series_description: str
    modality: str
    body_part: str
    series_date: str
    series_time: str
    bundle_key: str  # S3 key for the TAR bundle
    bundle_size: int  # Size of TAR file
    instance_count: int
    instances: List[InstanceInfo]
    # Aggregate info
    total_size: int
    first_instance_number: int
    last_instance_number: int

@dataclass
class StudyManifest:
    """Complete manifest for a DICOM study"""
    # Study identification
    study_uid: str
    study_date: str
    study_time: str
    study_description: str
    accession_number: str
    
    # Patient info (anonymized as needed)
    patient_id: str
    patient_name: str  # Can be anonymized
    patient_birth_date: Optional[str]
    patient_sex: Optional[str]
    
    # Study metadata
    institution_name: str
    referring_physician: str
    study_id: str
    
    # Storage information
    s3_bucket: str
    s3_prefix: str
    upload_timestamp: str
    total_size_bytes: int
    total_instances: int
    
    # Series information
    series_count: int
    series: List[SeriesInfo]
    
    # Index for quick lookups
    instance_index: Dict[str, Dict]  # instance_uid -> {series_uid, bundle_key, tar_offset}
    
    # Processing status
    status: str  # 'uploaded', 'processing', 'complete'
    processing_notes: Optional[Dict] = None
    
    # Versioning
    manifest_version: str = "2.0"
    created_by: str = "edge-gateway"

class ManifestGenerator:
    """Generate and manage DICOM study manifests"""
    
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
    
    def create_manifest(self, study_path: str, 
                       dicom_instances: List[str]) -> StudyManifest:
        """
        Create a complete manifest for a DICOM study
        """
        # Group instances by series
        series_map = self.group_by_series(dicom_instances)
        
        # Get study-level information from first instance
        first_ds = pydicom.dcmread(dicom_instances[0], stop_before_pixels=True)
        
        # Create study manifest
        manifest = StudyManifest(
            study_uid=str(first_ds.StudyInstanceUID),
            study_date=str(first_ds.StudyDate),
            study_time=str(first_ds.StudyTime),
            study_description=getattr(first_ds, 'StudyDescription', ''),
            accession_number=getattr(first_ds, 'AccessionNumber', ''),
            patient_id=str(first_ds.PatientID),
            patient_name=str(first_ds.PatientName),
            patient_birth_date=getattr(first_ds, 'PatientBirthDate', None),
            patient_sex=getattr(first_ds, 'PatientSex', None),
            institution_name=getattr(first_ds, 'InstitutionName', ''),
            referring_physician=getattr(first_ds, 'ReferringPhysicianName', ''),
            study_id=getattr(first_ds, 'StudyID', ''),
            s3_bucket=self.s3_bucket,
            s3_prefix=f"dicom/{first_ds.StudyDate}/{first_ds.PatientID}/{first_ds.StudyInstanceUID}",
            upload_timestamp=datetime.utcnow().isoformat(),
            total_size_bytes=0,
            total_instances=len(dicom_instances),
            series_count=len(series_map),
            series=[],
            instance_index={},
            status='uploading'
        )
        
        # Process each series
        for series_uid, instances in series_map.items():
            series_info = self.create_series_bundle(
                series_uid, 
                instances,
                manifest.s3_prefix
            )
            manifest.series.append(series_info)
            manifest.total_size_bytes += series_info.total_size
            
            # Build instance index for quick lookups
            for instance in series_info.instances:
                manifest.instance_index[instance.instance_uid] = {
                    'series_uid': series_uid,
                    'bundle_key': series_info.bundle_key,
                    'tar_offset': instance.tar_offset,
                    'tar_size': instance.tar_size,
                    'instance_number': instance.instance_number
                }
        
        return manifest
    
    def group_by_series(self, dicom_instances: List[str]) -> Dict[str, List[str]]:
        """Group DICOM instances by series UID"""
        series_map = {}
        
        for instance_path in dicom_instances:
            ds = pydicom.dcmread(instance_path, stop_before_pixels=True)
            series_uid = str(ds.SeriesInstanceUID)
            
            if series_uid not in series_map:
                series_map[series_uid] = []
            series_map[series_uid].append(instance_path)
        
        return series_map
    
    def create_series_bundle(self, series_uid: str, 
                            instances: List[str],
                            s3_prefix: str) -> SeriesInfo:
        """
        Create a TAR bundle for a series and upload to S3
        """
        # Get series metadata from first instance
        first_ds = pydicom.dcmread(instances[0], stop_before_pixels=True)
        
        series_info = SeriesInfo(
            series_uid=series_uid,
            series_number=getattr(first_ds, 'SeriesNumber', 0),
            series_description=getattr(first_ds, 'SeriesDescription', ''),
            modality=str(first_ds.Modality),
            body_part=getattr(first_ds, 'BodyPartExamined', ''),
            series_date=str(getattr(first_ds, 'SeriesDate', first_ds.StudyDate)),
            series_time=str(getattr(first_ds, 'SeriesTime', '')),
            bundle_key=f"{s3_prefix}/series_{series_uid}.tar",
            bundle_size=0,
            instance_count=len(instances),
            instances=[],
            total_size=0,
            first_instance_number=999999,
            last_instance_number=0
        )
        
        # Create TAR bundle in memory
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            for instance_path in sorted(instances):
                # Read DICOM file
                ds = pydicom.dcmread(instance_path)
                
                # Create instance info
                instance_info = InstanceInfo(
                    instance_uid=str(ds.SOPInstanceUID),
                    instance_number=int(getattr(ds, 'InstanceNumber', 0)),
                    sop_class_uid=str(ds.SOPClassUID),
                    file_size=Path(instance_path).stat().st_size,
                    pixel_data_offset=0,  # Will be set when reading TAR
                    pixel_data_size=len(ds.PixelData) if hasattr(ds, 'PixelData') else 0,
                    window_center=float(ds.WindowCenter) if hasattr(ds, 'WindowCenter') else None,
                    window_width=float(ds.WindowWidth) if hasattr(ds, 'WindowWidth') else None,
                    rows=int(ds.Rows) if hasattr(ds, 'Rows') else None,
                    columns=int(ds.Columns) if hasattr(ds, 'Columns') else None,
                    bits_allocated=int(ds.BitsAllocated) if hasattr(ds, 'BitsAllocated') else None,
                    photometric_interpretation=str(ds.PhotometricInterpretation) if hasattr(ds, 'PhotometricInterpretation') else None
                )
                
                # Track position in TAR file
                tar_info = tarfile.TarInfo(name=f"{instance_info.instance_number:04d}_{instance_info.instance_uid}.dcm")
                tar_info.size = instance_info.file_size
                instance_info.tar_offset = tar.fileobj.tell()
                
                # Add to TAR
                with open(instance_path, 'rb') as f:
                    tar.addfile(tar_info, f)
                
                instance_info.tar_size = tar.fileobj.tell() - instance_info.tar_offset
                
                # Update series info
                series_info.instances.append(instance_info)
                series_info.total_size += instance_info.file_size
                series_info.first_instance_number = min(series_info.first_instance_number, instance_info.instance_number)
                series_info.last_instance_number = max(series_info.last_instance_number, instance_info.instance_number)
        
        # Upload TAR bundle to S3
        tar_buffer.seek(0)
        series_info.bundle_size = len(tar_buffer.getvalue())
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=series_info.bundle_key,
            Body=tar_buffer.getvalue(),
            ContentType='application/x-tar',
            Metadata={
                'series-uid': series_uid,
                'modality': series_info.modality,
                'instance-count': str(series_info.instance_count)
            }
        )
        
        return series_info
    
    def save_manifest(self, manifest: StudyManifest):
        """Save manifest to S3"""
        manifest_key = f"{manifest.s3_prefix}/manifest.json"
        
        # Convert to JSON-serializable dict
        manifest_dict = self.manifest_to_dict(manifest)
        
        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest_dict, indent=2),
            ContentType='application/json',
            Metadata={
                'study-uid': manifest.study_uid,
                'study-date': manifest.study_date,
                'total-instances': str(manifest.total_instances)
            }
        )
        
        # Also save to DynamoDB for fast queries (optional)
        self.save_to_dynamodb(manifest)
        
        return manifest_key
    
    def manifest_to_dict(self, manifest: StudyManifest) -> dict:
        """Convert manifest to JSON-serializable dictionary"""
        return {
            'study_uid': manifest.study_uid,
            'study_date': manifest.study_date,
            'study_time': manifest.study_time,
            'study_description': manifest.study_description,
            'accession_number': manifest.accession_number,
            'patient_id': manifest.patient_id,
            'patient_name': manifest.patient_name,
            'patient_birth_date': manifest.patient_birth_date,
            'patient_sex': manifest.patient_sex,
            'institution_name': manifest.institution_name,
            'referring_physician': manifest.referring_physician,
            'study_id': manifest.study_id,
            's3_bucket': manifest.s3_bucket,
            's3_prefix': manifest.s3_prefix,
            'upload_timestamp': manifest.upload_timestamp,
            'total_size_bytes': manifest.total_size_bytes,
            'total_instances': manifest.total_instances,
            'series_count': manifest.series_count,
            'series': [
                {
                    'series_uid': s.series_uid,
                    'series_number': s.series_number,
                    'series_description': s.series_description,
                    'modality': s.modality,
                    'body_part': s.body_part,
                    'series_date': s.series_date,
                    'series_time': s.series_time,
                    'bundle_key': s.bundle_key,
                    'bundle_size': s.bundle_size,
                    'instance_count': s.instance_count,
                    'total_size': s.total_size,
                    'first_instance_number': s.first_instance_number,
                    'last_instance_number': s.last_instance_number,
                    'instances': [asdict(inst) for inst in s.instances]
                }
                for s in manifest.series
            ],
            'instance_index': manifest.instance_index,
            'status': manifest.status,
            'processing_notes': manifest.processing_notes,
            'manifest_version': manifest.manifest_version,
            'created_by': manifest.created_by
        }
    
    def save_to_dynamodb(self, manifest: StudyManifest):
        """
        Save manifest to DynamoDB for fast queries
        Optional but recommended for large-scale deployments
        """
        try:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('dicom-manifests')
            
            # Primary record
            table.put_item(Item={
                'study_uid': manifest.study_uid,
                'study_date': manifest.study_date,
                'patient_id': manifest.patient_id,
                'accession_number': manifest.accession_number,
                'modality': manifest.series[0].modality if manifest.series else '',
                's3_prefix': manifest.s3_prefix,
                'manifest_key': f"{manifest.s3_prefix}/manifest.json",
                'total_instances': manifest.total_instances,
                'total_size_mb': manifest.total_size_bytes / (1024 * 1024),
                'upload_timestamp': manifest.upload_timestamp,
                'ttl': int(datetime.utcnow().timestamp()) + (365 * 24 * 3600)  # 1 year TTL
            })
            
            # Secondary index for patient lookup
            table.put_item(Item={
                'patient_id': manifest.patient_id,
                'study_date': manifest.study_date,
                'study_uid': manifest.study_uid,
                'index_type': 'patient'
            })
            
        except Exception as e:
            print(f"DynamoDB save failed (non-critical): {e}")


class ManifestReader:
    """Read and query DICOM manifests"""
    
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
    
    def get_manifest(self, study_uid: str) -> Optional[Dict]:
        """Retrieve manifest for a study"""
        # Try DynamoDB first (fast)
        manifest_location = self.query_dynamodb(study_uid)
        
        if not manifest_location:
            # Fall back to S3 listing (slower)
            manifest_location = self.find_manifest_in_s3(study_uid)
        
        if manifest_location:
            # Load manifest from S3
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=manifest_location
            )
            return json.loads(response['Body'].read())
        
        return None
    
    def get_instance(self, study_uid: str, instance_uid: str) -> bytes:
        """
        Retrieve a specific DICOM instance from bundled storage
        """
        # Get manifest
        manifest = self.get_manifest(study_uid)
        if not manifest:
            raise ValueError(f"Study {study_uid} not found")
        
        # Find instance in index
        instance_info = manifest['instance_index'].get(instance_uid)
        if not instance_info:
            raise ValueError(f"Instance {instance_uid} not found")
        
        # Download TAR bundle
        response = self.s3_client.get_object(
            Bucket=self.s3_bucket,
            Key=instance_info['bundle_key'],
            Range=f"bytes={instance_info['tar_offset']}-{instance_info['tar_offset'] + instance_info['tar_size'] - 1}"
        )
        
        # Extract specific file from TAR chunk
        tar_chunk = response['Body'].read()
        
        # Parse TAR header and extract file
        with tarfile.open(fileobj=io.BytesIO(tar_chunk), mode='r') as tar:
            for member in tar:
                if instance_uid in member.name:
                    return tar.extractfile(member).read()
        
        raise ValueError(f"Instance {instance_uid} not found in bundle")
    
    def get_series(self, study_uid: str, series_uid: str) -> List[bytes]:
        """
        Retrieve all instances in a series
        """
        manifest = self.get_manifest(study_uid)
        if not manifest:
            raise ValueError(f"Study {study_uid} not found")
        
        # Find series
        for series in manifest['series']:
            if series['series_uid'] == series_uid:
                # Download entire TAR bundle
                response = self.s3_client.get_object(
                    Bucket=self.s3_bucket,
                    Key=series['bundle_key']
                )
                
                # Extract all files
                instances = []
                with tarfile.open(fileobj=io.BytesIO(response['Body'].read()), mode='r') as tar:
                    for member in tar:
                        if member.isfile():
                            instances.append(tar.extractfile(member).read())
                
                return instances
        
        raise ValueError(f"Series {series_uid} not found")
    
    def query_studies(self, 
                     patient_id: Optional[str] = None,
                     study_date: Optional[str] = None,
                     modality: Optional[str] = None,
                     accession_number: Optional[str] = None) -> List[Dict]:
        """
        Query studies using DynamoDB indices
        """
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('dicom-manifests')
        
        # Build query
        if patient_id:
            response = table.query(
                IndexName='patient-index',
                KeyConditionExpression='patient_id = :pid',
                ExpressionAttributeValues={':pid': patient_id}
            )
        elif accession_number:
            response = table.query(
                IndexName='accession-index',
                KeyConditionExpression='accession_number = :acc',
                ExpressionAttributeValues={':acc': accession_number}
            )
        else:
            # Scan with filters (slower)
            filter_expression = []
            expression_values = {}
            
            if study_date:
                filter_expression.append('study_date = :date')
                expression_values[':date'] = study_date
            if modality:
                filter_expression.append('modality = :mod')
                expression_values[':mod'] = modality
            
            response = table.scan(
                FilterExpression=' AND '.join(filter_expression) if filter_expression else None,
                ExpressionAttributeValues=expression_values if expression_values else None
            )
        
        return response.get('Items', [])
    
    def query_dynamodb(self, study_uid: str) -> Optional[str]:
        """Query DynamoDB for manifest location"""
        try:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('dicom-manifests')
            
            response = table.get_item(Key={'study_uid': study_uid})
            if 'Item' in response:
                return response['Item']['manifest_key']
        except:
            pass
        
        return None
    
    def find_manifest_in_s3(self, study_uid: str) -> Optional[str]:
        """Find manifest in S3 (slower fallback)"""
        # This is slower but works without DynamoDB
        prefix = f"dicom/"
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if study_uid in obj['Key'] and obj['Key'].endswith('manifest.json'):
                    return obj['Key']
        
        return None


# Example usage
if __name__ == "__main__":
    # Generate manifest during upload
    generator = ManifestGenerator(s3_bucket='my-dicom-bucket')
    
    # Create manifest for a study
    dicom_files = [
        '/path/to/instance1.dcm',
        '/path/to/instance2.dcm',
        # ... more instances
    ]
    
    manifest = generator.create_manifest('/tmp/study', dicom_files)
    manifest_key = generator.save_manifest(manifest)
    
    print(f"Manifest saved to: {manifest_key}")
    print(f"Total size: {manifest.total_size_bytes / (1024*1024):.2f} MB")
    print(f"Series count: {manifest.series_count}")
    print(f"Instance count: {manifest.total_instances}")
    
    # Read manifest for processing
    reader = ManifestReader(s3_bucket='my-dicom-bucket')
    
    # Get specific instance
    instance_data = reader.get_instance(
        study_uid='1.2.3.4.5',
        instance_uid='1.2.3.4.5.6.7'
    )
    
    # Query studies
    studies = reader.query_studies(patient_id='PAT123')
    for study in studies:
        print(f"Study: {study['study_uid']} - {study['study_date']}")
