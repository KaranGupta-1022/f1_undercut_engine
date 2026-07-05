FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend/ backend/

WORKDIR /app/backend

EXPOSE 5000

CMD ["python", "app.py"]
