FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    python-hl7==0.4.* \
    hl7apy==1.3.* \
    boto3==1.28.* \
    redis==5.0.* \
    requests==2.31.* \
    pydicom==2.4.* \
    flask==3.0.* \
    fhir.resources==7.0.*

# Copy application code
COPY hl7_processor.py /app/
COPY hl7_to_fhir_converter.py /app/
COPY config/ /app/config/

# Create non-root user
RUN useradd -m -u 1000 hl7user && chown -R hl7user:hl7user /app
USER hl7user

# Expose HL7 MLLP port
EXPOSE 2577

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import socket; s=socket.socket(); s.connect(('localhost', 2577)); s.close()" || exit 1

# Run the service
CMD ["python", "-u", "hl7_processor.py"]
