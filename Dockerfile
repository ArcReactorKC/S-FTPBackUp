FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 5000

ENV DATA_DIR=/data \
    DEVICE_DB=/data/devices.json \
    BACKUP_OUTPUT_DIR=/backups

CMD ["gunicorn", "-w", "2", "-k", "gthread", "--threads", "4", "-b", "0.0.0.0:5000", "--access-logfile", "-", "--error-logfile", "-", "--chdir", "/app/app", "app:app"]
