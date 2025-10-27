#!/usr/bin/env python3
"""
Simple HL7 Receiver - Edge Gateway
Receives HL7 messages and buffers them for cloud upload
No transformation or processing - just receive and forward
"""

import os
import socket
import ssl
import json
import uuid
import threading
import logging
from datetime import datetime
from pathlib import Path
import redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleHL7Receiver:
    def __init__(self):
        # Configuration
        self.mllp_port = int(os.getenv('MLLP_PORT', '2575'))
        self.mllp_tls_port = int(os.getenv('MLLP_TLS_PORT', '2576'))
        self.buffer_path = Path(os.getenv('BUFFER_PATH', '/app/buffer'))
        self.buffer_path.mkdir(parents=True, exist_ok=True)
        
        # Redis for tracking
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=6379,
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={
                1: 1,  # TCP_KEEPIDLE
                2: 1,  # TCP_KEEPINTVL
                3: 3,  # TCP_KEEPCNT
            }
        )
        
        # TLS configuration
        self.tls_enabled = os.getenv('TLS_ENABLED', 'true').lower() == 'true'
        self.tls_cert = os.getenv('TLS_CERT', '/app/certs/cert.pem')
        self.tls_key = os.getenv('TLS_KEY', '/app/certs/key.pem')
        
        logger.info(f"HL7 Receiver initialized - MLLP: {self.mllp_port}, TLS: {self.mllp_tls_port}")

    def start_servers(self):
        """Start both MLLP and MLLP/TLS servers"""
        # Start standard MLLP server
        mllp_thread = threading.Thread(
            target=self.run_mllp_server,
            args=(self.mllp_port, False),
            daemon=True
        )
        mllp_thread.start()
        
        # Start TLS MLLP server if enabled
        if self.tls_enabled:
            tls_thread = threading.Thread(
                target=self.run_mllp_server,
                args=(self.mllp_tls_port, True),
                daemon=True
            )
            tls_thread.start()
        
        # Keep main thread alive
        try:
            while True:
                threading.Event().wait(60)
                # Log stats every minute
                self.log_stats()
        except KeyboardInterrupt:
            logger.info("Shutting down HL7 Receiver")

    def run_mllp_server(self, port, use_tls):
        """Run MLLP server (with or without TLS)"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        if use_tls:
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(self.tls_cert, self.tls_key)
            server = context.wrap_socket(server, server_side=True)
        
        server.bind(('0.0.0.0', port))
        server.listen(5)
        
        logger.info(f"MLLP {'TLS ' if use_tls else ''}server listening on port {port}")
        
        while True:
            try:
                client_socket, address = server.accept()
                logger.info(f"Connection from {address} on port {port}")
                
                # Handle each connection in a thread
                thread = threading.Thread(
                    target=self.handle_connection,
                    args=(client_socket, address),
                    daemon=True
                )
                thread.start()
            except Exception as e:
                logger.error(f"Server error on port {port}: {e}")

    def handle_connection(self, client_socket, address):
        """Handle individual MLLP connection"""
        try:
            # Read MLLP message
            data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                # Check for MLLP end marker
                if b'\x1c\r' in data:
                    break
            
            # Process if valid MLLP message
            if data.startswith(b'\x0b') and data.endswith(b'\x1c\r'):
                # Extract HL7 message (remove MLLP wrapper)
                hl7_message = data[1:-2]
                
                # Save message to buffer
                message_id = self.save_message(hl7_message, address)
                
                # Generate simple ACK
                ack = self.generate_ack(hl7_message, message_id)
                
                # Send ACK with MLLP wrapper
                mllp_ack = b'\x0b' + ack + b'\x1c\r'
                client_socket.send(mllp_ack)
                
                logger.info(f"Message {message_id} received and buffered")
            else:
                logger.warning(f"Invalid MLLP message from {address}")
                
        except Exception as e:
            logger.error(f"Error handling connection from {address}: {e}")
        finally:
            client_socket.close()

    def save_message(self, hl7_message, source_address):
        """Save HL7 message to buffer for cloud upload"""
        try:
            # Generate unique ID
            message_id = str(uuid.uuid4())
            timestamp = datetime.utcnow()
            
            # Parse minimal info for tracking (no transformation)
            lines = hl7_message.decode('utf-8', errors='ignore').split('\r')
            msh_line = lines[0] if lines else ''
            segments = msh_line.split('|')
            
            message_type = segments[8] if len(segments) > 8 else 'UNKNOWN'
            message_control_id = segments[9] if len(segments) > 9 else message_id
            
            # Create metadata
            metadata = {
                'message_id': message_id,
                'message_control_id': message_control_id,
                'message_type': message_type,
                'source_ip': source_address[0],
                'source_port': source_address[1],
                'received_at': timestamp.isoformat(),
                'size_bytes': len(hl7_message)
            }
            
            # Save raw message to buffer
            message_file = self.buffer_path / f"hl7_{message_id}.hl7"
            message_file.write_bytes(hl7_message)
            
            # Save metadata
            metadata_file = self.buffer_path / f"hl7_{message_id}.json"
            metadata_file.write_text(json.dumps(metadata, indent=2))
            
            # Track in Redis queue for upload
            self.redis_client.lpush('hl7:upload_queue', message_id)
            self.redis_client.hset('hl7:metadata', message_id, json.dumps(metadata))
            self.redis_client.expire(f'hl7:metadata:{message_id}', 86400)  # 24 hour TTL
            
            # Update stats
            self.redis_client.hincrby('stats:hl7', 'total_received', 1)
            self.redis_client.hincrby('stats:hl7', f'type:{message_type}', 1)
            
            return message_id
            
        except Exception as e:
            logger.error(f"Error saving message: {e}")
            return str(uuid.uuid4())

    def generate_ack(self, hl7_message, message_id):
        """Generate minimal ACK response"""
        try:
            # Parse MSH segment for ACK generation
            lines = hl7_message.decode('utf-8', errors='ignore').split('\r')
            msh_line = lines[0] if lines else ''
            segments = msh_line.split('|')
            
            if len(segments) < 12:
                # Return generic ACK if can't parse
                return self.generic_ack(message_id)
            
            # Build ACK
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            ack_lines = [
                # MSH segment
                f"MSH|{segments[1]}|{segments[2]}|EDGE_GW|EDGE|{segments[3]}|{segments[4]}|"
                f"{timestamp}||ACK^{segments[8].split('^')[1] if '^' in segments[8] else 'A01'}|"
                f"ACK{message_id[:8]}|P|{segments[11]}",
                # MSA segment
                f"MSA|AA|{segments[9]}|Message received and queued for cloud upload"
            ]
            
            return '\r'.join(ack_lines).encode('utf-8')
            
        except Exception as e:
            logger.error(f"Error generating ACK: {e}")
            return self.generic_ack(message_id)

    def generic_ack(self, message_id):
        """Generate generic ACK when parsing fails"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return (
            f"MSH|^~\\&|EDGE_GW|EDGE|UNKNOWN|UNKNOWN|{timestamp}||ACK|"
            f"ACK{message_id[:8]}|P|2.5\r"
            f"MSA|AA|{message_id[:8]}|Message received"
        ).encode('utf-8')

    def log_stats(self):
        """Log statistics"""
        try:
            stats = self.redis_client.hgetall('stats:hl7')
            queue_length = self.redis_client.llen('hl7:upload_queue')
            
            if stats:
                logger.info(f"HL7 Stats - Total: {stats.get('total_received', 0)}, "
                           f"Queue: {queue_length}")
        except Exception as e:
            logger.error(f"Error logging stats: {e}")

if __name__ == "__main__":
    receiver = SimpleHL7Receiver()
    receiver.start_servers()
