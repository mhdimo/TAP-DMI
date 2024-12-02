# Build
FROM python:3.9-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Final
FROM python:3.9-slim

WORKDIR /app

# Copy necessary files from builder
COPY --from=builder /root/.local /root/.local
COPY irc_client.py .
COPY .env .

# Add Python packages to PATH
ENV PATH=/root/.local/bin:$PATH

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; socket.socket().connect(('kafka', 9092))"

# Run
CMD ["python", "irc_client.py"]