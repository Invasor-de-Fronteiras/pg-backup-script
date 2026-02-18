FROM python:3.12-slim

ARG PG_VERSION=16
ENV PG_VERSION=${PG_VERSION}

# Install pg_dump from the official PostgreSQL apt repository
RUN apt-get update && \
    apt-get install -y --no-install-recommends gnupg curl lsb-release && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client-${PG_VERSION} && \
    apt-get purge -y gnupg curl lsb-release && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backup.py .

CMD ["python", "backup.py"]
