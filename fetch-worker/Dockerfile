FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY fetch-worker/fetch_cloud.py /app/fetch_cloud.py
RUN pip install --no-cache-dir azure-identity azure-storage-blob azure-storage-queue
ENTRYPOINT ["python", "/app/fetch_cloud.py"]
