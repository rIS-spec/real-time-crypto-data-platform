FROM python:3.11-slim

WORKDIR /app

RUN addgroup --system appgroup && \
    adduser --system --no-create-home --ingroup appgroup appuser

COPY requirements.txt .

RUN pip install --no-cache-dir --timeout 1000 -r requirements.txt

COPY . .

RUN mkdir -p /app/data/raw /app/data/processed /app/logs && \
    chown -R appuser:appgroup /app/data /app/logs

USER appuser

EXPOSE 8000

CMD ["uvicorn", "api_service.main:app", "--host", "0.0.0.0", "--port", "8000"]