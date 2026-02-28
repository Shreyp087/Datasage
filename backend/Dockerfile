FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (needed for asyncpg / psycopg2 and network tools)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port (runtime depends on docker-compose command overriding this)
EXPOSE 8000
