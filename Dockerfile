# FROM python:3.11-slim

# RUN apt-get update \
#     && apt-get upgrade -y \
#     && apt-get install -y \
#         gcc \
#         libffi-dev \
#         libssl-dev \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

# # Create storage directories
# RUN mkdir -p /app/storage/html /app/storage/pdf \
#     && chmod -R 777 /app/storage

# # Create non-root user
# RUN useradd -m appuser
# USER appuser

# CMD ["scrapy", "crawl", "kedraspider"]

FROM python:3.11-slim

# Install system dependencies (keep this!)
RUN apt-get update \
    && apt-get install -y \
        gcc \
        libffi-dev \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

COPY . .

# Optional (only if you still write files)
RUN mkdir -p /app/storage/html /app/storage/pdf \
    && chmod -R 777 /app/storage

CMD ["dagster", "dev", "-m", "dagster_kedra.definitions", "-h", "0.0.0.0", "-p", "3000"]