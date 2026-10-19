FROM python:3.12.9-slim
RUN pip install --no-cache-dir duckdb==1.2.2
COPY worker.py /app/worker.py
COPY seed.py /app/seed.py
COPY inspect.py /app/inspect.py
ENTRYPOINT ["python", "/app/worker.py"]
