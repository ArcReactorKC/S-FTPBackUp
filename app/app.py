import json
import os
import shutil
import stat
import tempfile
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, jsonify, render_template, request
import paramiko

INTERVAL_SECONDS = {
    "1 hour": 60 * 60,
    "1 day": 60 * 60 * 24,
    "1 week": 60 * 60 * 24 * 7,
    "1 month": 60 * 60 * 24 * 30,
}

PLC_DIRECTORIES = [
    "/opt/plcnext/projects/",
    "/opt/plcnext/apps/",
    "/opt/plcnext/config/",
    "/opt/plcnext/data/",
]

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DEVICE_DB = Path(os.getenv("DEVICE_DB", DATA_DIR / "devices.json"))
BACKUP_OUTPUT_DIR = Path(os.getenv("BACKUP_OUTPUT_DIR", "/backups"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
scheduler = BackgroundScheduler()


def load_devices():
    if not DEVICE_DB.exists():
        return []
    with DEVICE_DB.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_devices(devices):
    DEVICE_DB.parent.mkdir(parents=True, exist_ok=True)
    with DEVICE_DB.open("w", encoding="utf-8") as handle:
        json.dump(devices, handle, indent=2)


def sftp_download_tree(sftp, remote_dir, local_dir):
    local_dir.mkdir(parents=True, exist_ok=True)
    try:
        entries = sftp.listdir_attr(remote_dir)
    except FileNotFoundError:
        return

    for entry in entries:
        name = entry.filename
        if name in (".", ".."):
            continue
        remote_path = f"{remote_dir.rstrip('/')}/{name}"
        local_path = local_dir / name
        if stat.S_ISDIR(entry.st_mode):
            sftp_download_tree(sftp, remote_path, local_path)
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            sftp.get(remote_path, str(local_path))


def create_backup(device):
    label = device["label"]
    ip_address = device["ip"]
    username = device["username"]
    password = device["password"]
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"{label}-{timestamp}"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / folder_name
        temp_path.mkdir(parents=True, exist_ok=True)
        transport = paramiko.Transport((ip_address, 22))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            for remote_dir in PLC_DIRECTORIES:
                target_dir = temp_path / remote_dir.strip("/")
                sftp_download_tree(sftp, remote_dir, target_dir)
        finally:
            sftp.close()
            transport.close()
        zip_path = BACKUP_OUTPUT_DIR / f"{folder_name}.zip"
        shutil.make_archive(zip_path.with_suffix(""), "zip", temp_path)


def schedule_device(device):
    interval = device["interval"]
    seconds = INTERVAL_SECONDS.get(interval)
    if not seconds:
        return
    job_id = f"backup-{device['label']}-{device['ip']}"
    scheduler.add_job(
        create_backup,
        trigger=IntervalTrigger(seconds=seconds),
        args=[device],
        id=job_id,
        replace_existing=True,
    )


def refresh_schedule():
    scheduler.remove_all_jobs()
    for device in load_devices():
        schedule_device(device)


@app.route("/")
def index():
    return render_template("index.html", intervals=sorted(INTERVAL_SECONDS.keys()))


@app.route("/devices", methods=["GET", "POST"])
def devices():
    if request.method == "GET":
        return jsonify(load_devices())

    payload = request.get_json(silent=True) or {}
    devices_payload = payload.get("devices", [])
    cleaned_devices = []
    for device in devices_payload:
        label = str(device.get("label", "")).strip()
        ip_address = str(device.get("ip", "")).strip()
        interval = str(device.get("interval", "")).strip()
        username = str(device.get("username", "")).strip()
        password = str(device.get("password", "")).strip()
        if (
            not label
            or not ip_address
            or interval not in INTERVAL_SECONDS
            or not username
            or not password
        ):
            continue
        cleaned_devices.append(
            {
                "label": label,
                "ip": ip_address,
                "interval": interval,
                "username": username,
                "password": password,
            }
        )

    save_devices(cleaned_devices)
    refresh_schedule()
    return jsonify({"status": "saved", "count": len(cleaned_devices)})


 codex/create-docker-with-plc-backup-interface-zc6dfu

codex/create-docker-with-plc-backup-interface-8tvd1f
 main
@app.route("/devices/<int:device_index>", methods=["DELETE"])
def delete_device(device_index):
    devices = load_devices()
    if device_index < 0 or device_index >= len(devices):
        return jsonify({"error": "Device not found"}), 404
    devices.pop(device_index)
    save_devices(devices)
    refresh_schedule()
    return jsonify({"status": "deleted"})


@app.route("/devices/<int:device_index>/backup", methods=["POST"])
def backup_device(device_index):
    devices = load_devices()
    if device_index < 0 or device_index >= len(devices):
        return jsonify({"error": "Device not found"}), 404
    create_backup(devices[device_index])
    return jsonify({"status": "backup_started"})


 codex/create-docker-with-plc-backup-interface-zc6dfu


 main
 main
if __name__ == "__main__":
    refresh_schedule()
    scheduler.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
