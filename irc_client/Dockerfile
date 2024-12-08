FROM python:3.9-slim

WORKDIR /app

RUN pip install irc python-dotenv requests

COPY ingestion.py .
COPY .env . 

CMD ["python", "irc_client/ingestion.py"]
