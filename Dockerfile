# Use Python 3.11
FROM python:3.11-slim

# Install System Dependencies (FFmpeg & Git)
# This installs FFmpeg at the OS level. No more startup scripts!
RUN apt-get update && \
    rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY . .

# Start the app (Gunicorn)
CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:8000", "--timeout", "600", "--workers", "1", "--threads", "8", "run:app"]