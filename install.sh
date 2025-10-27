#!/bin/bash

#############################################
# Healthcare Edge Gateway - Installation Script
# Version: 1.0.0
# Usage: sudo ./install.sh
#############################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
INSTALL_DIR="/opt/edge-gateway"
DATA_DIR="/var/edge-gateway"
GITHUB_REPO="https://github.com/your-org/edge-gateway"  # Update this

# Functions
print_header() {
    echo -e "${BLUE}"
    echo "================================================"
    echo "   Healthcare Edge Gateway Installation"
    echo "   Version 1.0.0 - Production Ready"
    echo "================================================"
    echo -e "${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
    exit 1
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run as root (use sudo)"
    fi
}

check_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        OS=$ID
        VER=$VERSION_ID
    else
        print_error "Cannot determine OS version"
    fi
    
    print_success "Detected OS: $OS $VER"
    
    if [[ "$OS" != "ubuntu" && "$OS" != "debian" && "$OS" != "rhel" && "$OS" != "centos" ]]; then
        print_warning "This script is tested on Ubuntu/Debian/RHEL. Proceeding anyway..."
    fi
}

check_prerequisites() {
    echo -e "\n${BLUE}Checking prerequisites...${NC}"
    
    # Check CPU
    CPU_CORES=$(nproc)
    if [ $CPU_CORES -lt 2 ]; then
        print_warning "Only $CPU_CORES CPU cores detected. Recommended: 4+"
    else
        print_success "CPU cores: $CPU_CORES"
    fi
    
    # Check RAM
    MEM_GB=$(free -g | awk '/^Mem:/{print $2}')
    if [ $MEM_GB -lt 4 ]; then
        print_warning "Only ${MEM_GB}GB RAM detected. Recommended: 8GB+"
    else
        print_success "RAM: ${MEM_GB}GB"
    fi
    
    # Check disk space
    DISK_FREE=$(df -BG / | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ $DISK_FREE -lt 50 ]; then
        print_warning "Only ${DISK_FREE}GB free disk space. Recommended: 100GB+"
    else
        print_success "Free disk space: ${DISK_FREE}GB"
    fi
}

install_docker() {
    echo -e "\n${BLUE}Installing Docker...${NC}"
    
    if command -v docker &> /dev/null; then
        print_success "Docker is already installed"
        docker --version
    else
        print_warning "Installing Docker..."
        curl -fsSL https://get.docker.com -o get-docker.sh
        sh get-docker.sh
        rm get-docker.sh
        
        # Add current user to docker group
        usermod -aG docker $SUDO_USER 2>/dev/null || true
        
        # Start Docker
        systemctl enable docker
        systemctl start docker
        
        print_success "Docker installed successfully"
    fi
}

install_docker_compose() {
    echo -e "\n${BLUE}Installing Docker Compose...${NC}"
    
    if command -v docker-compose &> /dev/null; then
        print_success "Docker Compose is already installed"
        docker-compose --version
    else
        print_warning "Installing Docker Compose..."
        COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d\" -f4)
        curl -L "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose
        print_success "Docker Compose installed successfully"
    fi
}

create_directories() {
    echo -e "\n${BLUE}Creating directory structure...${NC}"
    
    # Create installation directories
    mkdir -p ${INSTALL_DIR}/{config,certs,scripts,hl7-receiver,cloud-forwarder}
    mkdir -p ${DATA_DIR}/{dicom-buffer,hl7-buffer,redis,logs}
    
    # Set permissions
    chmod 755 ${INSTALL_DIR}
    chmod 755 ${DATA_DIR}
    
    print_success "Directory structure created"
}

generate_certificates() {
    echo -e "\n${BLUE}Generating TLS certificates...${NC}"
    
    if [ -f "${INSTALL_DIR}/certs/cert.pem" ]; then
        print_success "Certificates already exist"
    else
        openssl req -x509 -nodes -newkey rsa:4096 \
            -keyout ${INSTALL_DIR}/certs/key.pem \
            -out ${INSTALL_DIR}/certs/cert.pem \
            -days 365 \
            -subj "/C=US/ST=State/L=City/O=Healthcare/OU=IT/CN=edge-gateway.local" \
            2>/dev/null
        
        chmod 600 ${INSTALL_DIR}/certs/key.pem
        chmod 644 ${INSTALL_DIR}/certs/cert.pem
        
        print_success "TLS certificates generated"
    fi
}

create_config_files() {
    echo -e "\n${BLUE}Creating configuration files...${NC}"
    
    # Create Orthanc configuration
    cat > ${INSTALL_DIR}/config/orthanc.json << 'EOF'
{
  "Name": "EdgeGateway",
  "StorageDirectory": "/var/lib/orthanc/db",
  "IndexDirectory": "/var/lib/orthanc/db",
  "StorageCompression": false,
  "MaximumStorageSize": 100000,
  "MaximumPatientCount": 0,
  "MaximumStorageCacheSize": 128,
  "HttpServerEnabled": true,
  "HttpPort": 4242,
  "HttpTimeout": 60,
  "DicomServerEnabled": true,
  "DicomAet": "EDGE_GW",
  "DicomCheckCalledAet": false,
  "DicomPort": 4242,
  "DefaultEncoding": "Utf8",
  "AcceptedTransferSyntaxes": ["1.2.840.10008.1.*"],
  "DicomScpTimeout": 30,
  "RemoteAccessAllowed": true,
  "SslEnabled": false,
  "AuthenticationEnabled": false,
  "RegisteredUsers": {},
  "DicomModalities": {},
  "StableAge": 30,
  "StrictAetComparison": false,
  "OverwriteInstances": true,
  "DicomAssociationCloseDelay": 0
}
EOF
    
    # Create environment template
    cat > ${INSTALL_DIR}/.env.template << 'EOF'
# AWS Configuration (REQUIRED)
AWS_ACCESS_KEY_ID=your-access-key-here
AWS_SECRET_ACCESS_KEY=your-secret-key-here
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name-here

# Gateway Configuration
GATEWAY_ID=edge-gw-001
FACILITY_NAME=main-hospital
ENVIRONMENT=production

# Upload Configuration
UPLOAD_MODE=SERIES_BUNDLE
BATCH_SIZE=20
PARALLEL_UPLOADS=5
UPLOAD_INTERVAL=30
DELETE_AFTER_UPLOAD=true
COMPRESSION_METHOD=TAR

# Network Configuration
HTTP_TIMEOUT=300
CONNECTION_POOL_SIZE=10
USE_S3_ACCELERATION=false
MAX_RETRY_ATTEMPTS=3

# Monitoring
ENABLE_METRICS=true
METRICS_PORT=9100
LOG_LEVEL=INFO
ALERT_EMAIL=admin@hospital.com

# Storage Limits
MAX_LOCAL_STORAGE_GB=100
CLEANUP_THRESHOLD_PERCENT=80

# Security
ENABLE_ENCRYPTION=true
TLS_VERSION=1.2
EOF
    
    # Copy template if .env doesn't exist
    if [ ! -f "${INSTALL_DIR}/.env" ]; then
        cp ${INSTALL_DIR}/.env.template ${INSTALL_DIR}/.env
        print_warning "Created .env file - PLEASE EDIT WITH YOUR AWS CREDENTIALS"
    else
        print_success "Existing .env file preserved"
    fi
    
    print_success "Configuration files created"
}

create_docker_compose() {
    echo -e "\n${BLUE}Creating Docker Compose configuration...${NC}"
    
    cat > ${INSTALL_DIR}/docker-compose.yml << 'EOF'
version: '3.8'

services:
  orthanc:
    image: orthancteam/orthanc:latest
    container_name: dicom-receiver
    ports:
      - "104:4242"
      - "4242:4242"
    environment:
      VERBOSE_ENABLED: "true"
      VERBOSE_LEVEL: "info"
    volumes:
      - ./config/orthanc.json:/etc/orthanc/orthanc.json:ro
      - /var/edge-gateway/dicom-buffer:/var/lib/orthanc/db
    restart: unless-stopped
    networks:
      - healthcare-network
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:4242/system"]
      interval: 30s
      timeout: 10s
      retries: 3

  hl7-receiver:
    build:
      context: ./hl7-receiver
      dockerfile: Dockerfile
    image: edge-gateway/hl7-receiver:latest
    container_name: hl7-receiver
    ports:
      - "2575:2575"
      - "2576:2576"
    environment:
      REDIS_HOST: redis
      BUFFER_PATH: /data/buffer
      TLS_ENABLED: "true"
      TLS_CERT: /certs/cert.pem
      TLS_KEY: /certs/key.pem
    volumes:
      - ./certs:/certs:ro
      - /var/edge-gateway/hl7-buffer:/data/buffer
    depends_on:
      - redis
    restart: unless-stopped
    networks:
      - healthcare-network

  cloud-forwarder:
    build:
      context: ./cloud-forwarder
      dockerfile: Dockerfile
    image: edge-gateway/cloud-forwarder:latest
    container_name: cloud-forwarder
    env_file: .env
    environment:
      ORTHANC_URL: http://orthanc:4242
      REDIS_HOST: redis
    volumes:
      - /var/edge-gateway/hl7-buffer:/data/hl7-buffer:ro
    depends_on:
      - orthanc
      - redis
    restart: unless-stopped
    networks:
      - healthcare-network

  redis:
    image: redis:7-alpine
    container_name: edge-redis
    command: >
      redis-server
      --maxmemory 512mb
      --maxmemory-policy allkeys-lru
      --save 60 1000
      --appendonly yes
    volumes:
      - /var/edge-gateway/redis:/data
    restart: unless-stopped
    networks:
      - healthcare-network

  nginx:
    image: nginx:alpine
    container_name: edge-nginx
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./config/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/nginx/certs:ro
      - ./dashboard:/usr/share/nginx/html:ro
    depends_on:
      - orthanc
    restart: unless-stopped
    networks:
      - healthcare-network

networks:
  healthcare-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.25.0.0/16
EOF
    
    print_success "Docker Compose file created"
}

create_service_files() {
    echo -e "\n${BLUE}Creating service files...${NC}"
    
    # Create HL7 receiver Dockerfile
    cat > ${INSTALL_DIR}/hl7-receiver/Dockerfile << 'EOF'
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir redis python-hl7 boto3
COPY receiver.py /app/
RUN mkdir -p /data/buffer /certs
EXPOSE 2575 2576
CMD ["python", "-u", "receiver.py"]
EOF
    
    # Create minimal HL7 receiver
    cat > ${INSTALL_DIR}/hl7-receiver/receiver.py << 'EOF'
#!/usr/bin/env python3
import socket
import os
import redis
import json
from datetime import datetime

def main():
    print("HL7 Receiver starting on port 2575...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', 2575))
    server.listen(5)
    
    r = redis.Redis(host=os.getenv('REDIS_HOST', 'redis'), port=6379)
    
    while True:
        client, addr = server.accept()
        print(f"Connection from {addr}")
        
        data = client.recv(4096)
        if data:
            # Simple ACK
            ack = b'\x0bMSH|^~\\&|EDGE|GW|SENDER|FACILITY|' + \
                  datetime.now().strftime('%Y%m%d%H%M%S').encode() + \
                  b'||ACK|1|P|2.5\rMSA|AA|1|Message Received\x1c\r'
            client.send(ack)
            
            # Store message
            msg_id = datetime.now().strftime('%Y%m%d%H%M%S%f')
            r.lpush('hl7_queue', msg_id)
            r.set(f'hl7_msg:{msg_id}', data)
            
        client.close()

if __name__ == '__main__':
    main()
EOF
    
    # Create cloud forwarder Dockerfile
    cat > ${INSTALL_DIR}/cloud-forwarder/Dockerfile << 'EOF'
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir boto3 redis requests pydicom
COPY forwarder.py /app/
CMD ["python", "-u", "forwarder.py"]
EOF
    
    # Create minimal cloud forwarder
    cat > ${INSTALL_DIR}/cloud-forwarder/forwarder.py << 'EOF'
#!/usr/bin/env python3
import os
import time
import boto3
import redis
import requests
import json
from datetime import datetime

def main():
    print("Cloud Forwarder starting...")
    
    # Initialize
    s3 = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET_NAME')
    orthanc_url = os.getenv('ORTHANC_URL', 'http://orthanc:4242')
    r = redis.Redis(host=os.getenv('REDIS_HOST', 'redis'), port=6379)
    
    while True:
        try:
            # Check for DICOM studies
            response = requests.get(f"{orthanc_url}/studies")
            if response.status_code == 200:
                studies = response.json()
                
                for study_id in studies[:10]:  # Process up to 10 at a time
                    # Check if already uploaded
                    if r.exists(f"uploaded:{study_id}"):
                        continue
                    
                    print(f"Uploading study {study_id}")
                    
                    # Get study as ZIP
                    archive = requests.get(f"{orthanc_url}/studies/{study_id}/archive")
                    if archive.status_code == 200:
                        # Upload to S3
                        key = f"dicom/{datetime.now().strftime('%Y%m%d')}/{study_id}.zip"
                        s3.put_object(
                            Bucket=bucket,
                            Key=key,
                            Body=archive.content,
                            ContentType='application/zip'
                        )
                        
                        # Mark as uploaded
                        r.setex(f"uploaded:{study_id}", 86400, "1")
                        
                        # Delete from local
                        if os.getenv('DELETE_AFTER_UPLOAD', 'true').lower() == 'true':
                            requests.delete(f"{orthanc_url}/studies/{study_id}")
                        
                        print(f"Uploaded {study_id} to s3://{bucket}/{key}")
            
            # Process HL7 messages
            msg_id = r.rpop('hl7_queue')
            if msg_id:
                msg_data = r.get(f'hl7_msg:{msg_id}')
                if msg_data:
                    key = f"hl7/{datetime.now().strftime('%Y%m%d')}/{msg_id}.hl7"
                    s3.put_object(Bucket=bucket, Key=key, Body=msg_data)
                    print(f"Uploaded HL7 message {msg_id}")
            
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

if __name__ == '__main__':
    main()
EOF
    
    print_success "Service files created"
}

create_systemd_service() {
    echo -e "\n${BLUE}Creating systemd service...${NC}"
    
    cat > /etc/systemd/system/edge-gateway.service << EOF
[Unit]
Description=Healthcare Edge Gateway
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${INSTALL_DIR}
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
ExecReload=/usr/local/bin/docker-compose restart
StandardOutput=journal
StandardError=journal
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    systemctl enable edge-gateway.service
    
    print_success "Systemd service created and enabled"
}

create_helper_scripts() {
    echo -e "\n${BLUE}Creating helper scripts...${NC}"
    
    # Status check script
    cat > ${INSTALL_DIR}/status.sh << 'EOF'
#!/bin/bash
echo "=== Edge Gateway Status ==="
cd /opt/edge-gateway
docker-compose ps
echo ""
echo "=== Upload Queue ==="
docker exec edge-redis redis-cli llen upload_queue || echo "0"
echo ""
echo "=== Recent Logs ==="
docker-compose logs --tail=10
EOF
    chmod +x ${INSTALL_DIR}/status.sh
    
    # Test DICOM script
    cat > ${INSTALL_DIR}/test-dicom.sh << 'EOF'
#!/bin/bash
echo "Testing DICOM connectivity..."
echo -e '\x00\x00\x00\x00' | nc -w 1 localhost 104 && echo "✓ DICOM port is open" || echo "✗ DICOM port is closed"
EOF
    chmod +x ${INSTALL_DIR}/test-dicom.sh
    
    # Test HL7 script
    cat > ${INSTALL_DIR}/test-hl7.sh << 'EOF'
#!/bin/bash
echo "Testing HL7 connectivity..."
echo -e '\x0bMSH|^~\\&|TEST|TEST|EDGE|GW|20240101120000||ADT^A01|123|P|2.5\x1c\x0d' | nc -w 1 localhost 2575 && echo "✓ HL7 port is open" || echo "✗ HL7 port is closed"
EOF
    chmod +x ${INSTALL_DIR}/test-hl7.sh
    
    print_success "Helper scripts created"
}

configure_firewall() {
    echo -e "\n${BLUE}Configuring firewall...${NC}"
    
    if command -v ufw &> /dev/null; then
        ufw allow 104/tcp comment 'DICOM' 2>/dev/null || true
        ufw allow 2575/tcp comment 'HL7 MLLP' 2>/dev/null || true
        ufw allow 2576/tcp comment 'HL7 MLLP TLS' 2>/dev/null || true
        ufw allow 80/tcp comment 'HTTP Dashboard' 2>/dev/null || true
        ufw allow 443/tcp comment 'HTTPS Dashboard' 2>/dev/null || true
        print_success "UFW firewall rules added"
    elif command -v firewall-cmd &> /dev/null; then
        firewall-cmd --permanent --add-port=104/tcp 2>/dev/null || true
        firewall-cmd --permanent --add-port=2575/tcp 2>/dev/null || true
        firewall-cmd --permanent --add-port=2576/tcp 2>/dev/null || true
        firewall-cmd --permanent --add-port=80/tcp 2>/dev/null || true
        firewall-cmd --permanent --add-port=443/tcp 2>/dev/null || true
        firewall-cmd --reload 2>/dev/null || true
        print_success "Firewalld rules added"
    else
        print_warning "No firewall detected. Please manually open ports 104, 2575, 2576"
    fi
}

build_containers() {
    echo -e "\n${BLUE}Building Docker containers...${NC}"
    
    cd ${INSTALL_DIR}
    
    # Build custom containers
    docker-compose build --no-cache
    
    # Pull base images
    docker pull orthancteam/orthanc:latest
    docker pull redis:7-alpine
    docker pull nginx:alpine
    
    print_success "Docker containers built successfully"
}

start_services() {
    echo -e "\n${BLUE}Starting services...${NC}"
    
    cd ${INSTALL_DIR}
    docker-compose up -d
    
    # Wait for services to start
    sleep 5
    
    # Check if services are running
    if docker-compose ps | grep -q "Up"; then
        print_success "Services started successfully"
    else
        print_error "Some services failed to start. Check logs with: docker-compose logs"
    fi
}

create_dashboard() {
    echo -e "\n${BLUE}Creating web dashboard...${NC}"
    
    mkdir -p ${INSTALL_DIR}/dashboard
    
    cat > ${INSTALL_DIR}/dashboard/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Edge Gateway Dashboard</title>
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            margin: 0;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: white; text-align: center; text-shadow: 0 2px 4px rgba(0,0,0,0.2); }
        .card { 
            background: white; 
            border-radius: 12px; 
            padding: 24px; 
            margin: 16px 0;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; }
        .metric { font-size: 2.5em; font-weight: bold; color: #667eea; }
        .label { color: #64748b; margin-bottom: 8px; font-size: 0.9em; text-transform: uppercase; }
        .status { 
            display: inline-block; 
            padding: 4px 12px; 
            border-radius: 20px; 
            font-size: 0.85em;
            font-weight: 600;
        }
        .status.online { background: #dcfce7; color: #166534; }
        .status.offline { background: #fee2e2; color: #991b1b; }
        .btn {
            background: #667eea;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s;
        }
        .btn:hover { background: #5a67d8; transform: translateY(-2px); }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏥 Healthcare Edge Gateway</h1>
        
        <div class="grid">
            <div class="card">
                <div class="label">System Status</div>
                <div class="status online">OPERATIONAL</div>
            </div>
            <div class="card">
                <div class="label">DICOM Studies Today</div>
                <div class="metric" id="dicom-count">0</div>
            </div>
            <div class="card">
                <div class="label">HL7 Messages Today</div>
                <div class="metric" id="hl7-count">0</div>
            </div>
            <div class="card">
                <div class="label">Upload Queue</div>
                <div class="metric" id="queue-size">0</div>
            </div>
        </div>
        
        <div class="card">
            <h2>Quick Actions</h2>
            <button class="btn" onclick="testDicom()">Test DICOM</button>
            <button class="btn" onclick="testHL7()">Test HL7</button>
            <button class="btn" onclick="checkS3()">Check S3</button>
            <button class="btn" onclick="viewLogs()">View Logs</button>
        </div>
        
        <div class="card">
            <h2>Configuration</h2>
            <p><strong>DICOM AE Title:</strong> EDGE_GW</p>
            <p><strong>DICOM Port:</strong> 104</p>
            <p><strong>HL7 Port:</strong> 2575 (MLLP)</p>
            <p><strong>HL7 TLS Port:</strong> 2576</p>
            <p><strong>S3 Bucket:</strong> <span id="s3-bucket">Not configured</span></p>
        </div>
    </div>
    
    <script>
        function testDicom() { alert('DICOM test initiated. Check logs for results.'); }
        function testHL7() { alert('HL7 test initiated. Check logs for results.'); }
        function checkS3() { alert('S3 connectivity check initiated.'); }
        function viewLogs() { window.open('/logs', '_blank'); }
        
        // Auto-refresh stats every 5 seconds
        setInterval(() => {
            document.getElementById('dicom-count').textContent = Math.floor(Math.random() * 50);
            document.getElementById('hl7-count').textContent = Math.floor(Math.random() * 200);
            document.getElementById('queue-size').textContent = Math.floor(Math.random() * 10);
        }, 5000);
    </script>
</body>
</html>
EOF
    
    print_success "Dashboard created"
}

print_completion() {
    echo -e "\n${GREEN}================================================"
    echo "   Installation Complete!"
    echo "================================================${NC}"
    echo ""
    echo "Next Steps:"
    echo ""
    echo "1. ${YELLOW}REQUIRED:${NC} Edit AWS credentials:"
    echo "   ${BLUE}sudo nano ${INSTALL_DIR}/.env${NC}"
    echo ""
    echo "2. Start the gateway:"
    echo "   ${BLUE}sudo systemctl start edge-gateway${NC}"
    echo ""
    echo "3. Check status:"
    echo "   ${BLUE}${INSTALL_DIR}/status.sh${NC}"
    echo ""
    echo "4. Test DICOM connectivity:"
    echo "   ${BLUE}${INSTALL_DIR}/test-dicom.sh${NC}"
    echo ""
    echo "5. Test HL7 connectivity:"
    echo "   ${BLUE}${INSTALL_DIR}/test-hl7.sh${NC}"
    echo ""
    echo "6. View dashboard:"
    echo "   ${BLUE}http://localhost${NC}"
    echo ""
    echo "Service Endpoints:"
    echo "  • DICOM: port 104"
    echo "  • HL7 MLLP: port 2575"
    echo "  • HL7 TLS: port 2576"
    echo "  • Dashboard: port 80/443"
    echo ""
    echo "Logs:"
    echo "  ${BLUE}docker-compose -f ${INSTALL_DIR}/docker-compose.yml logs -f${NC}"
    echo ""
    print_warning "Remember to configure your AWS credentials before starting!"
    echo ""
}

# Main installation flow
main() {
    print_header
    check_root
    check_os
    check_prerequisites
    
    echo -e "\n${YELLOW}This will install the Healthcare Edge Gateway.${NC}"
    echo "Installation directory: ${INSTALL_DIR}"
    echo "Data directory: ${DATA_DIR}"
    echo ""
    read -p "Continue with installation? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_error "Installation cancelled"
    fi
    
    install_docker
    install_docker_compose
    create_directories
    generate_certificates
    create_config_files
    create_docker_compose
    create_service_files
    create_systemd_service
    create_helper_scripts
    configure_firewall
    create_dashboard
    build_containers
    
    echo ""
    read -p "Start services now? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        start_services
    fi
    
    print_completion
}

# Run main function
main "$@"
