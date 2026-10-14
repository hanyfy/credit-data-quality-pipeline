FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY tests ./tests
COPY pytest.ini .
ENV PYTHONPATH=/app/src
CMD ["sh", "-c", "python -m dq.generate --rows 100000 && python -m dq.run"]
