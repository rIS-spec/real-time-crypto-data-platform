# Base image — Python 3.11 slim keeps the image small
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements first (Docker caches this layer — faster rebuilds)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your actual application code
COPY . .

# Create directories for data and logs
RUN mkdir -p /app/data/raw /app/data/processed /app/logs

# Expose the port FastAPI runs on
EXPOSE 8000

# Command to start the FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]







# docker build -t data-platform .
# docker run -p 8000:8000 data-platform
# docker run -d -p 8000:8000 data-platform
# docker run -d -p 8000:8000 --name data-platform data-platform

