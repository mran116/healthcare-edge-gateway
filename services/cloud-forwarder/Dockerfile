FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    boto3==1.28.* \
    requests==2.31.* \
    redis==5.0.* \
    pydicom==2.4.* \
    python-dotenv==1.0.*

# Copy application code
COPY s3_forwarder.py /app/
COPY config/ /app/config/

# Create cache directory
RUN mkdir -p /app/cache

# Create non-root user
RUN useradd -m -u 1000 dicom && chown -R dicom:dicom /app
USER dicom

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

# Run the service
CMD ["python", "-u", "s3_forwarder.py"]
