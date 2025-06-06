# Use lightweight Python image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Install system-level dependencies (optional, but useful for image/PDF generation, etc.)
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Expose port (optional, for documentation)
EXPOSE 8000

# Run FastAPI app with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]