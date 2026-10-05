FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY ingestion /app/ingestion
COPY fts/production/serving/api /app/fts/production/serving/api
RUN pip install --no-cache-dir '.[azure,parquet]'
ENTRYPOINT ["python", "-m", "ingestion.orchestration_cloud"]
