FROM openjdk:11-slim

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_VERSION=3.0.0
ENV SPARK_HOME=/opt/spark

# Download and install Spark (using archive.apache.org)
RUN cd /tmp && \
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3.2 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

# Add Spark bin to PATH
ENV PATH=$PATH:${SPARK_HOME}/bin

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy Spark application
COPY sparkConsumer.py .

# Set the proper environment variables for PySpark
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}

CMD ["python", "sparkConsumer.py"]
