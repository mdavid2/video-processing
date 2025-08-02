FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y ffmpeg libsm6 libxext6

# Set working directory
WORKDIR /app

# Copy files
COPY . /app

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Install curl
RUN apt-get update && apt-get install -y curl

# Download YOLOv8 model
RUN mkdir -p /models && \
    curl -L -o yolov8n-pose.pt https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n-pose.pt

# clean installtions
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Create data folders
RUN mkdir -p /data/to_be_processed /data/processed/pose_data

# Set volume mount point
VOLUME ["/data"]

# Run the script
CMD ["python", "main.py"]
