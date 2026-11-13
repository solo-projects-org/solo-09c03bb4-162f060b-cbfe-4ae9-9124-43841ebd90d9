FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV DATA_DIR=/data \
    GEN_CHUNK_ROWS=100 \
    GEN_CHUNK_DELAY_MS=100 \
    SLOW_CHUNK_BYTES=1024 \
    SLOW_CHUNK_DELAY_MS=300

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
