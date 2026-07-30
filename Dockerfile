FROM python:3.10-slim

WORKDIR /app

# Copy requirement files or install directly
RUN pip install --no-cache-dir \
    kafka-python-ng \
    snowflake-connector-python \
    cryptography

COPY . /app

CMD ["python3", "-u", "consumer.py"]