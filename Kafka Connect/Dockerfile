# Dockerfile for IRC Client
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy script and environment file
COPY irc_client.py /app/irc_client.py
COPY .env /app/.env

# Install dependencies
RUN pip install irc confluent-kafka python-dotenv

# Run the script
CMD ["python", "/app/irc_client.py"]