import fcntl
import json
import os
import shutil
import stat
import tempfile
import threading
import warnings
from datetime import datetime, timezone
from ftplib import FTP, all_errors as ftp_errors
from pathlib import Path
from typing import Optional

from cryptography.utils import CryptographyDeprecationWarning

warnings.filterwarnings(
    "ignore",
    message=(
        r".*TripleDES has been moved to cryptography\.hazmat\.decrepit\.ciphers\.algorithms\.TripleDES.*"
    ),
    category=CryptographyDeprecationWarning,
)

import paramiko
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, jsonify, render_template, request

INTERVAL_SECONDS = {
    "1 hour": 60 * 60,
    "1 day": 60 * 60 * 24,
    "1 week": 60 * 60 * 24 * 7,
    "1 month": 60 * 60 * 24 * 30,
}

# Fallback directories (used when a device has no custom paths selected)
PLC_DIRECTORIES = ["/"]

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DEVICE_DB = Path(os.getenv("DEVICE_DB", str(DATA_DIR / "devices.json")))
BACKUP_OUTPUT_DIR = Path(os.getenv("BACKUP_OUTPUT_DIR", "/backups"))
SFTP_PORT_DEFAULT = int(os.getenv("SFTP_PORT", "22"))
FTP_PORT_DEFAULT = int(os.getenv("FTP_PORT", "21"))
DEFAULT_MAX_BACKUPS = 10

DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.static_folder = "static"
scheduler = BackgroundScheduler()
backup_status_lock = threading.Lock()
backup_status = {}
_scheduler_lock_handle = None
_scheduler_started = False


def load_devices():
    if not DEVICE_DB.exists():
        return []
    with DEVICE_DB.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_devices(devices):
    DEVICE_DB.parent.mkdir(parents=True, exist_ok=True)
    with DEVICE_DB.open("w", encoding="utf-8") as handle:
        json.dump(devices, handle, indent=2)


def _open_sftp(ip_address: str, username: str, password: str, port: int):
    """Open an SFTP session with host-key auto-accept for unattended backups."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=ip_address,
        port=port,
        username=username,
        password=password,
        timeout=10,
        banner_timeout=10,
        auth_timeout=10,
    )
    sftp = ssh.open_sftp()
    return ssh, sftp


def _open_ftp(ip_address: str, username: str, password: str, port: int) -> FTP:
    ftp = FTP()
    ftp.connect(host=ip_address, port=port, timeout=10)
    ftp.login(user=username, passwd=password)
    return ftp


def sftp_download_tree(sftp, remote_dir: str, local_dir: Path):
    """Recursively download a remote directory tree. Skips unreadable paths."""
    local_dir.mkdir(parents=True, exist_ok=True)

    try:
        entries = sftp.listdir_attr(remote_dir)
    except FileNotFoundError:
        print(f"[SFTP] Missing remote dir: {remote_dir}")
        return
    except PermissionError as e:
        print(f"[SFTP] Permission denied listing dir {remote_dir}: {e}")
        return
    except OSError as e:
        print(f"[SFTP] Cannot list dir {remote_dir}: {e}")
        return

    for entry in entries:
        name = entry.filename
        if name in (".", ".."):
            continue

        # PLCnext often exposes a special 'current' pointer that fails on read; skip it.
        if name == "current":
            continue

        remote_path = f"{remote_dir.rstrip('/')}/{name}"
        local_path = local_dir / name

        try:
            if stat.S_ISDIR(entry.st_mode):
                sftp_download_tree(sftp, remote_path, local_path)
            else:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                sftp.get(remote_path, str(local_path))
        except PermissionError as e:
            print(f"[SFTP] Permission denied: {remote_path} ({e})")
        except OSError as e:
            # Common on embedded SFTP servers: generic "Failure" for protected/special files.
            print(f"[SFTP] Read failed: {remote_path} ({e})")
        except Exception as e:
            print(f"[SFTP] Error on {remote_path}: {e}")


def _ftp_entries(ftp: FTP, remote_dir: str):
    try:
        return [
            {"name": name, "is_dir": facts.get("type") == "dir"}
            for name, facts in ftp.mlsd(remote_dir)
            if name not in (".", "..")
        ]
    except ftp_errors:
        current = ftp.pwd()
        entries = []
        try:
            ftp.cwd(remote_dir)
            names = ftp.nlst()
            for name in names:
                name = Path(name).name
                if name in (".", ".."):
                    continue
                is_dir = False
                try:
                    ftp.cwd(name)
                    is_dir = True
                except ftp_errors:
                    is_dir = False
                finally:
                    ftp.cwd(remote_dir)
                entries.append({"name": name, "is_dir": is_dir})
        except ftp_errors as e:
            print(f"[FTP] Cannot list dir {remote_dir}: {e}")
        finally:
            try:
                ftp.cwd(current)
            except ftp_errors:
                pass
        return entries


def ftp_download_tree(ftp: FTP, remote_dir: str, local_dir: Path):
    local_dir.mkdir(parents=True, exist_ok=True)
    try:
        entries = _ftp_entries(ftp, remote_dir)
    except ftp_errors as e:
        print(f"[FTP] Missing or unreadable dir {remote_dir}: {e}")
        return

    for entry in entries:
        name = entry["name"]
        remote_path = f"{remote_dir.rstrip('/')}/{name}"
        local_path = local_dir / name
        if entry["is_dir"]:
            ftp_download_tree(ftp, remote_path, local_path)
        else:
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with local_path.open("wb") as handle:
                    ftp.retrbinary(f"RETR {remote_path}", handle.write)
            except ftp_errors as e:
                print(f"[FTP] Read failed: {remote_path} ({e})")


def _device_key(device: dict) -> str:
    return _job_id_for_device(device)


def set_backup_status(device: dict, state: str, detail: Optional[str] = None):
    entry = {
        "state": state,
        "detail": detail or "",
        "updated_at": datetime.now().isoformat(),
    }
    with backup_status_lock:
        backup_status[_device_key(device)] = entry


def get_backup_status(device: dict):
    with backup_status_lock:
        return backup_status.get(_device_key(device))


def create_backup(device: dict, status_callback=None):
    label = device["label"]
    ip_address = device["ip"]
    username = device["username"]
    password = device["password"]
    protocol = str(device.get("protocol") or "sftp").lower()
    port_default = FTP_PORT_DEFAULT if protocol == "ftp" else SFTP_PORT_DEFAULT
    port = int(device.get("port") or port_default)

    if status_callback:
        status_callback(device, "connecting", f"{protocol.upper()} session")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"{label}-{timestamp}"

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / folder_name
        temp_path.mkdir(parents=True, exist_ok=True)

        directories = device.get("paths") or PLC_DIRECTORIES
        if status_callback:
            status_callback(device, "downloading", f"{len(directories)} paths")
        if protocol == "ftp":
            ftp = _open_ftp(ip_address, username, password, port)
            try:
                for remote_dir in directories:
                    remote_dir = str(remote_dir).strip()
                    if not remote_dir:
                        continue
                    target_dir = temp_path / remote_dir.strip("/")
                    ftp_download_tree(ftp, remote_dir, target_dir)
            finally:
                try:
                    ftp.quit()
                except ftp_errors:
                    ftp.close()
        else:
            ssh, sftp = _open_sftp(ip_address, username, password, port)
            try:
                for remote_dir in directories:
                    remote_dir = str(remote_dir).strip()
                    if not remote_dir:
                        continue
                    target_dir = temp_path / remote_dir.strip("/")
                    sftp_download_tree(sftp, remote_dir, target_dir)
            finally:
                try:
                    sftp.close()
                finally:
                    ssh.close()

        if status_callback:
            status_callback(device, "archiving", "Creating zip archive")

        base_name = str((BACKUP_OUTPUT_DIR / folder_name).with_suffix(""))
        shutil.make_archive(base_name, "zip", temp_path)

    max_backups = int(device.get("max_backups") or DEFAULT_MAX_BACKUPS)
    prune_old_backups(device["label"], max_backups)


def prune_old_backups(label: str, max_backups: int):
    safe_max = max(1, min(int(max_backups), 100))
    backups = sorted(
        BACKUP_OUTPUT_DIR.glob(f"{label}-*.zip"),
        key=lambda path: path.stat().st_mtime,
    )
    while len(backups) > safe_max:
        oldest = backups.pop(0)
        try:
            oldest.unlink()
        except FileNotFoundError:
            continue


def _job_id_for_device(device: dict) -> str:
    # Keep this stable; it’s how we look up next_run_time
    return f"backup-{device['label']}-{device['ip']}"


def _get_job_next_run_time(job):
    if not job:
        return None
    try:
        return getattr(job, "next_run_time", None)
    except Exception:
        return None


def get_next_run_time_for_device(device: dict):
    job = scheduler.get_job(_job_id_for_device(device))
    next_run_time = _get_job_next_run_time(job)
    if not next_run_time:
        return None
    # ISO format is easy for the browser to parse & display
    return next_run_time.isoformat()


def _parse_iso_datetime(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _update_saved_next_run_time(device: dict):
    next_run = get_next_run_time_for_device(device)
    devices_list = load_devices()
    target_key = _device_key(device)
    updated = False
    for saved in devices_list:
        if _device_key(saved) == target_key:
            saved["next_run_at"] = next_run
            updated = True
            break
    if updated:
        save_devices(devices_list)


def run_backup_and_record(device: dict):
    set_backup_status(device, "starting", "Preparing backup")
    try:
        create_backup(device, status_callback=set_backup_status)
        set_backup_status(device, "completed", "Backup complete")
    except Exception as exc:
        set_backup_status(device, "failed", str(exc))
        raise
    finally:
        _update_saved_next_run_time(device)


def schedule_device(device: dict):
    interval = device.get("interval")
    seconds = INTERVAL_SECONDS.get(interval)
    if not seconds:
        return
    start_date = _parse_iso_datetime(device.get("next_run_at"))
    if start_date:
        now = datetime.now(tz=start_date.tzinfo) if start_date.tzinfo else datetime.now()
        if start_date <= now:
            start_date = None
        elif start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)

    job = scheduler.add_job(
        run_backup_and_record,
        trigger=IntervalTrigger(seconds=seconds, start_date=start_date),
        args=[device],
        id=_job_id_for_device(device),
        replace_existing=True,
    )
    next_run_time = _get_job_next_run_time(job)
    if next_run_time:
        device["next_run_at"] = next_run_time.isoformat()


def refresh_schedule():
    scheduler.remove_all_jobs()
    devices_list = load_devices()
    for device in devices_list:
        schedule_device(device)
    save_devices(devices_list)


def start_scheduler():
    global _scheduler_lock_handle, _scheduler_started
    if _scheduler_started:
        return
    lock_path = DATA_DIR / "scheduler.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_file.close()
        return
    _scheduler_lock_handle = lock_file
    refresh_schedule()
    scheduler.start()
    _scheduler_started = True


@app.before_request
def _start_scheduler_once():
    start_scheduler()


@app.route("/")
def index():
    return render_template("index.html", intervals=sorted(INTERVAL_SECONDS.keys()))


@app.route("/devices", methods=["GET", "POST"])
def devices():
    if request.method == "GET":
        devices_list = load_devices()
        for d in devices_list:
            d["next_backup"] = get_next_run_time_for_device(d) or d.get("next_run_at")
            d["backup_status"] = get_backup_status(d)
        return jsonify(devices_list)

    payload = request.get_json(silent=True) or {}
    devices_payload = payload.get("devices", [])
    cleaned_devices = []
    existing_devices = {
        _device_key(device): device for device in load_devices()
    }

    for device in devices_payload:
        label = str(device.get("label", "")).strip()
        ip_address = str(device.get("ip", "")).strip()
        interval = str(device.get("interval", "")).strip()
        username = str(device.get("username", "")).strip()
        password = str(device.get("password", "")).strip()
        protocol = str(device.get("protocol") or "sftp").lower()
        port_default = FTP_PORT_DEFAULT if protocol == "ftp" else SFTP_PORT_DEFAULT
        port = int(device.get("port") or port_default)
        max_backups_raw = device.get("max_backups")
        try:
            max_backups = int(max_backups_raw) if max_backups_raw is not None else DEFAULT_MAX_BACKUPS
        except (TypeError, ValueError):
            max_backups = DEFAULT_MAX_BACKUPS
        max_backups = max(1, min(max_backups, 100))

        if (
            not label
            or not ip_address
            or interval not in INTERVAL_SECONDS
            or not username
            or not password
        ):
            continue

        paths = device.get("paths") or []
        cleaned_paths = [str(path).strip() for path in paths if str(path).strip()]

        cleaned_device = {
            "label": label,
            "ip": ip_address,
            "interval": interval,
            "username": username,
            "password": password,
            "protocol": protocol,
            "port": port,
            "paths": cleaned_paths,
            "max_backups": max_backups,
        }

        existing = existing_devices.get(_device_key(cleaned_device))
        if existing and existing.get("interval") == interval:
            cleaned_device["next_run_at"] = existing.get("next_run_at")

        cleaned_devices.append(cleaned_device)

    save_devices(cleaned_devices)
    refresh_schedule()
    return jsonify({"status": "saved", "count": len(cleaned_devices)})


@app.route("/devices/<int:device_index>", methods=["DELETE"])
def delete_device(device_index: int):
    devices_list = load_devices()
    if device_index < 0 or device_index >= len(devices_list):
        return jsonify({"error": "Device not found"}), 404
    devices_list.pop(device_index)
    save_devices(devices_list)
    refresh_schedule()
    return jsonify({"status": "deleted"})


@app.route("/devices/<int:device_index>/backup", methods=["POST"])
def backup_device(device_index: int):
    devices_list = load_devices()
    if device_index < 0 or device_index >= len(devices_list):
        return jsonify({"error": "Device not found"}), 404
    device = devices_list[device_index]
    set_backup_status(device, "queued", "Backup queued")
    thread = threading.Thread(target=run_backup_and_record, args=(device,), daemon=True)
    thread.start()
    return jsonify({"status": "backup_started"})


@app.route("/browse", methods=["POST"])
def browse():
    payload = request.get_json(silent=True) or {}
    ip_address = str(payload.get("ip", "")).strip()
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", "")).strip()
    path = str(payload.get("path", "")).strip() or "/"
    protocol = str(payload.get("protocol") or "sftp").lower()
    port_default = FTP_PORT_DEFAULT if protocol == "ftp" else SFTP_PORT_DEFAULT
    port = int(payload.get("port") or port_default)

    if not ip_address or not username or not password:
        return jsonify({"path": path, "entries": []})

    entries = []
    try:
        if protocol == "ftp":
            ftp = _open_ftp(ip_address, username, password, port)
            try:
                entries = _ftp_entries(ftp, path)
            finally:
                try:
                    ftp.quit()
                except ftp_errors:
                    ftp.close()
        else:
            ssh, sftp = _open_sftp(ip_address, username, password, port)
            try:
                for entry in sftp.listdir_attr(path):
                    name = entry.filename
                    if name in (".", ".."):
                        continue
                    entries.append({"name": name, "is_dir": stat.S_ISDIR(entry.st_mode)})
            finally:
                try:
                    sftp.close()
                finally:
                    ssh.close()
    except (PermissionError, FileNotFoundError, OSError, paramiko.SSHException, ftp_errors) as e:
        print(f"[BROWSE] {protocol.upper()} failed for {ip_address}:{port} {path}: {e}")
        entries = []

    # Sort: directories first, then files; alphabetically within each group
    entries.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
    return jsonify({"path": path, "entries": entries})


if __name__ == "__main__":
    start_scheduler()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
