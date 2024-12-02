FROM apache/spark-py:v3.4.0

USER root

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY sentiment_analyzer.py .

CMD ["python", "sentiment_analyzer.py"]