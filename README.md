# PLCNext Backup Scheduler

This container provides a small web UI to register PLC devices and schedule SFTP backups of key PLCNext directories.

## Build and run

```bash
docker build -t plcnext-backup .

docker run -p 5000:5000 \
  -e BACKUP_OUTPUT_DIR=/backups \
  -v $(pwd)/backups:/backups \
  -v $(pwd)/data:/data \
  plcnext-backup
```

Open `http://localhost:5000` to add devices.

## Environment variables

- `BACKUP_OUTPUT_DIR`: Where zipped backups are stored (default `/backups`).
- `DATA_DIR`: Internal storage for configuration (default `/data`).
- `DEVICE_DB`: Override the path to the devices JSON file.
- SFTP credentials are stored per device in the web UI and connect on port 22.

## Backup contents

For each device and interval, the container downloads over SFTP:

- `/opt/plcnext/projects/`
- `/opt/plcnext/apps/`
- `/opt/plcnext/config/`
- `/opt/plcnext/data/`

These are stored in a timestamped folder and zipped as `label-YYYY-MM-DD_HH-MM-SS.zip` in the output directory.
