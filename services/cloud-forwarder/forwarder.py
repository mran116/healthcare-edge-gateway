#!/usr/bin/env python3
"""
Optimized Cloud Forwarder - Handles multiple images efficiently
Supports both individual image uploads and batch study uploads
"""

import os
import json
import time
import boto3
import redis
import requests
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import threading
from queue import Queue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OptimizedCloudForwarder:
    def __init__(self):
        # AWS Configuration
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        # S3 client with optimized configuration
        config = Config(
            region_name=self.aws_region,
            signature_version='v4',
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50  # Increased for parallel uploads
        )
        
        self.s3_client = boto3.client('s3', config=config)
        
        # Multipart upload configuration for large files
        self.transfer_config = TransferConfig(
            multipart_threshold=1024 * 25,  # 25MB
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True
        )
        
        # Service endpoints
        self.orthanc_url = os.getenv('ORTHANC_URL', 'http://orthanc:4242')
        self.hl7_buffer_path = Path('/app/hl7-buffer')
        
        # Upload configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '10'))
        self.upload_interval = int(os.getenv('UPLOAD_INTERVAL', '30'))
        self.delete_after_upload = os.getenv('DELETE_AFTER_UPLOAD', 'true').lower() == 'true'
        self.upload_mode = os.getenv('UPLOAD_MODE', 'study')  # 'study' or 'instance'
        self.parallel_uploads = int(os.getenv('PARALLEL_UPLOADS', '5'))
        self.compress_uploads = os.getenv('COMPRESS_UPLOADS', 'false').lower() == 'true'
        
        # Redis for tracking
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=6379,
            decode_responses=True,
            socket_keepalive=True
        )
        
        # Thread pool for parallel uploads
        self.executor = ThreadPoolExecutor(max_workers=self.parallel_uploads)
        
        # Upload queue for managing multiple uploads
        self.upload_queue = Queue()
        
        # Statistics
        self.stats = {
            'images_uploaded': 0,
            'studies_uploaded': 0,
            'bytes_uploaded': 0,
            'upload_errors': 0
        }
        
        logger.info(f"Optimized Cloud Forwarder initialized - Mode: {self.upload_mode}, "
                   f"Parallel uploads: {self.parallel_uploads}")

    def run(self):
        """Main processing loop"""
        logger.info("Starting Optimized Cloud Forwarder Service")
        
        # Start upload workers
        for i in range(self.parallel_uploads):
            worker_thread = threading.Thread(target=self.upload_worker, daemon=True)
            worker_thread.start()
        
        while True:
            try:
                # Process DICOM studies/instances
                self.process_dicom_data()
                
                # Process HL7 messages in batch
                self.process_hl7_batch()
                
                # Log statistics
                self.log_statistics()
                
                # Wait before next cycle
                time.sleep(self.upload_interval)
                
            except KeyboardInterrupt:
                logger.info("Shutting down Cloud Forwarder")
                self.executor.shutdown(wait=True)
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(self.upload_interval)

    def process_dicom_data(self):
        """Process DICOM data based on upload mode"""
        if self.upload_mode == 'study':
            self.process_dicom_studies()
        else:
            self.process_dicom_instances()

    def process_dicom_studies(self):
        """Upload complete DICOM studies (multiple images as one unit)"""
        try:
            # Get list of studies from Orthanc
            response = requests.get(f"{self.orthanc_url}/studies")
            if response.status_code != 200:
                return
            
            study_ids = response.json()
            logger.info(f"Found {len(study_ids)} studies to process")
            
            # Process in batches
            for i in range(0, min(len(study_ids), self.batch_size), self.parallel_uploads):
                batch = study_ids[i:i+self.parallel_uploads]
                futures = []
                
                for study_id in batch:
                    # Check if already uploaded
                    if self.redis_client.exists(f"uploaded:dicom:study:{study_id}"):
                        continue
                    
                    # Submit to thread pool
                    future = self.executor.submit(self.upload_dicom_study_optimized, study_id)
                    futures.append(future)
                
                # Wait for batch to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Upload failed: {e}")
                        self.stats['upload_errors'] += 1
                        
        except Exception as e:
            logger.error(f"Error processing DICOM studies: {e}")

    def process_dicom_instances(self):
        """Upload individual DICOM instances (each image separately)"""
        try:
            # Get list of instances from Orthanc
            response = requests.get(f"{self.orthanc_url}/instances")
            if response.status_code != 200:
                return
            
            instance_ids = response.json()
            logger.info(f"Found {len(instance_ids)} instances to process")
            
            # Group instances by study for organized upload
            instances_by_study = {}
            for instance_id in instance_ids[:self.batch_size * 10]:  # Limit for performance
                # Check if already uploaded
                if self.redis_client.exists(f"uploaded:dicom:instance:{instance_id}"):
                    continue
                
                # Get instance metadata
                meta_response = requests.get(f"{self.orthanc_url}/instances/{instance_id}/tags")
                if meta_response.status_code == 200:
                    tags = meta_response.json()
                    study_uid = tags.get('0020,000d', {}).get('Value', ['unknown'])[0]
                    
                    if study_uid not in instances_by_study:
                        instances_by_study[study_uid] = []
                    instances_by_study[study_uid].append(instance_id)
            
            # Upload instances in parallel
            futures = []
            for study_uid, instance_ids in instances_by_study.items():
                for instance_id in instance_ids:
                    future = self.executor.submit(
                        self.upload_dicom_instance, 
                        instance_id, 
                        study_uid
                    )
                    futures.append(future)
                    
                    # Limit concurrent uploads
                    if len(futures) >= self.parallel_uploads:
                        for completed in as_completed(futures[:self.parallel_uploads]):
                            completed.result()
                        futures = futures[self.parallel_uploads:]
            
            # Wait for remaining uploads
            for future in as_completed(futures):
                future.result()
                
        except Exception as e:
            logger.error(f"Error processing DICOM instances: {e}")

    def upload_dicom_study_optimized(self, study_id: str):
        """Upload complete study with all images"""
        try:
            start_time = time.time()
            
            # Get study metadata
            metadata_response = requests.get(f"{self.orthanc_url}/studies/{study_id}")
            if metadata_response.status_code != 200:
                return
            
            metadata = metadata_response.json()
            
            # Extract key information
            patient_id = metadata.get('PatientMainDicomTags', {}).get('PatientID', 'unknown')
            study_date = metadata.get('MainDicomTags', {}).get('StudyDate', 'unknown')
            study_uid = metadata.get('MainDicomTags', {}).get('StudyInstanceUID', study_id)
            modality = metadata.get('MainDicomTags', {}).get('Modality', 'unknown')
            instance_count = len(metadata.get('Instances', []))
            
            logger.info(f"Uploading study {study_id} with {instance_count} images")
            
            # Generate S3 path
            s3_prefix = f"dicom/{study_date}/{patient_id}/{study_uid}"
            
            # Option 1: Upload as ZIP archive (fastest for multiple images)
            if self.compress_uploads or instance_count > 10:
                archive_response = requests.get(
                    f"{self.orthanc_url}/studies/{study_id}/archive",
                    stream=True
                )
                
                if archive_response.status_code == 200:
                    s3_key = f"{s3_prefix}/study_archive.zip"
                    
                    # Upload with multipart for large studies
                    self.s3_client.upload_fileobj(
                        archive_response.raw,
                        self.s3_bucket,
                        s3_key,
                        Config=self.transfer_config,
                        ExtraArgs={
                            'ContentType': 'application/zip',
                            'Metadata': {
                                'patient-id': patient_id,
                                'study-date': study_date,
                                'study-uid': study_uid,
                                'modality': modality,
                                'instance-count': str(instance_count),
                                'upload-time': datetime.utcnow().isoformat()
                            },
                            'StorageClass': 'INTELLIGENT_TIERING'
                        }
                    )
                    
                    upload_size = int(archive_response.headers.get('content-length', 0))
                    
            # Option 2: Upload instances individually (better for processing)
            else:
                instances = metadata.get('Instances', [])
                upload_size = 0
                
                for instance_id in instances:
                    # Download instance
                    instance_response = requests.get(
                        f"{self.orthanc_url}/instances/{instance_id}/file",
                        stream=True
                    )
                    
                    if instance_response.status_code == 200:
                        # Get instance number for naming
                        tags_response = requests.get(
                            f"{self.orthanc_url}/instances/{instance_id}/simplified-tags"
                        )
                        tags = tags_response.json() if tags_response.status_code == 200 else {}
                        instance_number = tags.get('InstanceNumber', instance_id[:8])
                        sop_instance_uid = tags.get('SOPInstanceUID', instance_id)
                        
                        s3_key = f"{s3_prefix}/instances/{instance_number:04d}_{sop_instance_uid}.dcm"
                        
                        # Upload instance
                        self.s3_client.upload_fileobj(
                            instance_response.raw,
                            self.s3_bucket,
                            s3_key,
                            Config=self.transfer_config,
                            ExtraArgs={
                                'ContentType': 'application/dicom',
                                'StorageClass': 'INTELLIGENT_TIERING'
                            }
                        )
                        
                        upload_size += int(instance_response.headers.get('content-length', 0))
            
            # Upload metadata JSON
            metadata_key = f"{s3_prefix}/metadata.json"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
            
            # Mark as uploaded
            self.redis_client.setex(
                f"uploaded:dicom:study:{study_id}",
                86400 * 7,  # Keep record for 7 days
                json.dumps({
                    's3_prefix': s3_prefix,
                    'uploaded_at': datetime.utcnow().isoformat(),
                    'instance_count': instance_count,
                    'size_bytes': upload_size
                })
            )
            
            # Update statistics
            self.stats['studies_uploaded'] += 1
            self.stats['images_uploaded'] += instance_count
            self.stats['bytes_uploaded'] += upload_size
            
            upload_time = time.time() - start_time
            upload_speed = upload_size / (1024 * 1024 * upload_time) if upload_time > 0 else 0
            
            logger.info(f"Uploaded study {study_id}: {instance_count} images, "
                       f"{upload_size/(1024*1024):.2f} MB in {upload_time:.2f}s "
                       f"({upload_speed:.2f} MB/s)")
            
            # Delete from Orthanc if configured
            if self.delete_after_upload:
                requests.delete(f"{self.orthanc_url}/studies/{study_id}")
                logger.info(f"Deleted study {study_id} from local storage")
                
        except Exception as e:
            logger.error(f"Error uploading study {study_id}: {e}")
            self.redis_client.sadd('failed:dicom:studies', study_id)
            raise

    def upload_dicom_instance(self, instance_id: str, study_uid: str):
        """Upload single DICOM instance"""
        try:
            # Download instance
            instance_response = requests.get(
                f"{self.orthanc_url}/instances/{instance_id}/file",
                stream=True
            )
            
            if instance_response.status_code != 200:
                return
            
            # Get instance metadata
            tags_response = requests.get(
                f"{self.orthanc_url}/instances/{instance_id}/simplified-tags"
            )
            tags = tags_response.json() if tags_response.status_code == 200 else {}
            
            patient_id = tags.get('PatientID', 'unknown')
            study_date = tags.get('StudyDate', 'unknown')
            instance_number = tags.get('InstanceNumber', '0')
            sop_instance_uid = tags.get('SOPInstanceUID', instance_id)
            
            # Generate S3 key
            s3_key = f"dicom/{study_date}/{patient_id}/{study_uid}/instances/{instance_number:04d}_{sop_instance_uid}.dcm"
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                instance_response.raw,
                self.s3_bucket,
                s3_key,
                Config=self.transfer_config,
                ExtraArgs={
                    'ContentType': 'application/dicom',
                    'Metadata': {
                        'patient-id': patient_id,
                        'study-uid': study_uid,
                        'instance-uid': sop_instance_uid,
                        'instance-number': instance_number
                    },
                    'StorageClass': 'INTELLIGENT_TIERING'
                }
            )
            
            # Mark as uploaded
            self.redis_client.setex(
                f"uploaded:dicom:instance:{instance_id}",
                86400 * 7,
                s3_key
            )
            
            self.stats['images_uploaded'] += 1
            
            # Delete from Orthanc if configured
            if self.delete_after_upload:
                requests.delete(f"{self.orthanc_url}/instances/{instance_id}")
                
        except Exception as e:
            logger.error(f"Error uploading instance {instance_id}: {e}")
            raise

    def process_hl7_batch(self):
        """Process HL7 messages in batch for efficiency"""
        try:
            # Get multiple messages from queue
            messages_to_upload = []
            
            for _ in range(self.batch_size * 5):  # Get more HL7 messages per batch
                message_id = self.redis_client.rpop('hl7:upload_queue')
                if not message_id:
                    break
                messages_to_upload.append(message_id)
            
            if not messages_to_upload:
                return
            
            logger.info(f"Processing batch of {len(messages_to_upload)} HL7 messages")
            
            # Group by date and type for efficient upload
            messages_by_group = {}
            for message_id in messages_to_upload:
                metadata = self.get_hl7_metadata(message_id)
                if metadata:
                    group_key = f"{metadata['received_at'][:10]}_{metadata['message_type']}"
                    if group_key not in messages_by_group:
                        messages_by_group[group_key] = []
                    messages_by_group[group_key].append((message_id, metadata))
            
            # Upload each group
            for group_key, messages in messages_by_group.items():
                self.upload_hl7_group(group_key, messages)
                
        except Exception as e:
            logger.error(f"Error processing HL7 batch: {e}")

    def upload_hl7_group(self, group_key: str, messages: List[tuple]):
        """Upload a group of HL7 messages together"""
        try:
            date_part, type_part = group_key.split('_')
            
            # Create a batch upload
            batch_data = []
            for message_id, metadata in messages:
                message_file = self.hl7_buffer_path / f"hl7_{message_id}.hl7"
                if message_file.exists():
                    batch_data.append({
                        'id': message_id,
                        'content': message_file.read_text(),
                        'metadata': metadata
                    })
            
            if batch_data:
                # Upload as a batch JSON file
                batch_key = f"hl7/{date_part}/{type_part}/batch_{datetime.utcnow().timestamp()}.json"
                
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=batch_key,
                    Body=json.dumps(batch_data, indent=2),
                    ContentType='application/json',
                    StorageClass='INTELLIGENT_TIERING'
                )
                
                logger.info(f"Uploaded HL7 batch: {len(batch_data)} messages to {batch_key}")
                
                # Mark all as uploaded and clean up
                for message_id, _ in messages:
                    self.redis_client.setex(
                        f"uploaded:hl7:{message_id}",
                        86400 * 7,
                        batch_key
                    )
                    
                    if self.delete_after_upload:
                        message_file = self.hl7_buffer_path / f"hl7_{message_id}.hl7"
                        metadata_file = self.hl7_buffer_path / f"hl7_{message_id}.json"
                        message_file.unlink(missing_ok=True)
                        metadata_file.unlink(missing_ok=True)
                
                self.stats['images_uploaded'] += len(batch_data)  # Count HL7 as "images" for stats
                
        except Exception as e:
            logger.error(f"Error uploading HL7 group: {e}")
            # Put messages back in queue
            for message_id, _ in messages:
                self.redis_client.lpush('hl7:upload_queue', message_id)

    def get_hl7_metadata(self, message_id: str) -> Optional[Dict]:
        """Get HL7 message metadata"""
        try:
            metadata_file = self.hl7_buffer_path / f"hl7_{message_id}.json"
            if metadata_file.exists():
                return json.loads(metadata_file.read_text())
            
            # Try Redis
            metadata_str = self.redis_client.hget('hl7:metadata', message_id)
            if metadata_str:
                return json.loads(metadata_str)
                
        except Exception as e:
            logger.error(f"Error getting HL7 metadata: {e}")
        
        return None

    def upload_worker(self):
        """Worker thread for processing upload queue"""
        while True:
            try:
                item = self.upload_queue.get(timeout=1)
                if item:
                    # Process upload item
                    self.process_upload_item(item)
                    self.upload_queue.task_done()
            except:
                continue

    def process_upload_item(self, item):
        """Process individual upload item"""
        # Implementation for queue-based uploads
        pass

    def log_statistics(self):
        """Log upload statistics"""
        logger.info(f"Statistics - Studies: {self.stats['studies_uploaded']}, "
                   f"Images: {self.stats['images_uploaded']}, "
                   f"Data: {self.stats['bytes_uploaded']/(1024*1024*1024):.2f} GB, "
                   f"Errors: {self.stats['upload_errors']}")
        
        # Save to Redis for monitoring
        for key, value in self.stats.items():
            self.redis_client.hset('stats:uploads', key, value)

if __name__ == "__main__":
    forwarder = OptimizedCloudForwarder()
    forwarder.run()
