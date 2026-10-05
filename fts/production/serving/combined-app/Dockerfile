FROM quickwit/quickwit:0.9.0
USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/serving-venv
WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY ingestion /app/ingestion
COPY fts/__init__.py /app/fts/__init__.py
COPY fts/production/__init__.py /app/fts/production/__init__.py
COPY fts/production/serving/__init__.py /app/fts/production/serving/__init__.py
COPY fts/production/serving/api /app/fts/production/serving/api
RUN /opt/serving-venv/bin/pip install --no-cache-dir '.[azure,router,mcp]'
COPY fts/production/serving/searcher-node.yaml /quickwit/serving-node.yaml
COPY fts/production/serving/combined-app/start.sh /usr/local/bin/start-serving
RUN chmod +x /usr/local/bin/start-serving
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/start-serving"]
