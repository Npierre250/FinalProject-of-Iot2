#!/usr/bin/env python3
"""
simulate_factory_data.py
Simulates industrial IoT sensor data.
  - stream mode : opens a TCP socket and streams records indefinitely
  - batch  mode : writes a large CSV to HDFS /flink/input/
  - hdfs   mode : writes one CSV batch then exits (used by Docker init)

Kept compatible with the original field schema.
"""

import os
import sys
import time
import csv
import io
import random
import socket
import logging
import subprocess
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ── Machine catalogue ─────────────────────────────────────────────────────────
MACHINE_TYPES = ["CNC_Mill", "Lathe", "Drill_Press", "Conveyor", "Robot_Arm", "Compressor"]

MACHINE_PROFILES = {
    "CNC_Mill":    dict(temp=(55, 90), vib=(5, 30),  sound=(60, 95), power=(10, 50)),
    "Lathe":       dict(temp=(50, 85), vib=(3, 25),  sound=(55, 90), power=(8,  40)),
    "Drill_Press": dict(temp=(45, 80), vib=(4, 28),  sound=(58, 92), power=(5,  25)),
    "Conveyor":    dict(temp=(30, 65), vib=(1, 15),  sound=(50, 75), power=(3,  20)),
    "Robot_Arm":   dict(temp=(40, 75), vib=(2, 20),  sound=(45, 70), power=(7,  35)),
    "Compressor":  dict(temp=(60, 95), vib=(8, 35),  sound=(65, 100),power=(15, 60)),
}

COLUMNS = [
    "machine_id", "machine_type", "operating_hours",
    "temperature_c", "vibration_mm_s", "sound_db",
    "power_kw", "maintenance_required", "error_count",
    "last_maintenance_days", "production_rate", "fault_detected",
    "timestamp"
]


def make_record(machine_id: str, machine_type: str, op_hours: float, ts: datetime) -> dict:
    profile = MACHINE_PROFILES[machine_type]
    fault_chance = min(0.05 + op_hours / 10000, 0.40)
    fault = random.random() < fault_chance

    temp = random.uniform(*profile["temp"])
    vib  = random.uniform(*profile["vib"])
    snd  = random.uniform(*profile["sound"])
    pwr  = random.uniform(*profile["power"])

    if fault:
        temp  *= random.uniform(1.05, 1.25)
        vib   *= random.uniform(1.10, 1.40)
        snd   *= random.uniform(1.05, 1.20)

    return {
        "machine_id":            machine_id,
        "machine_type":          machine_type,
        "operating_hours":       round(op_hours, 1),
        "temperature_c":         round(temp, 2),
        "vibration_mm_s":        round(vib, 2),
        "sound_db":              round(snd, 2),
        "power_kw":              round(pwr, 2),
        "maintenance_required":  fault or op_hours > 8000,
        "error_count":           random.randint(0, 5) if fault else 0,
        "last_maintenance_days": random.randint(0, 365),
        "production_rate":       round(random.uniform(60, 100), 1),
        "fault_detected":        fault,
        "timestamp":             ts.isoformat(),
    }


def record_to_csv_line(r: dict) -> str:
    return ",".join(str(r[c]) for c in COLUMNS)


# ── HDFS helpers ──────────────────────────────────────────────────────────────
HDFS_INPUT = os.getenv("HDFS_INPUT", "hdfs://namenode:9000/flink/input")
NAMENODE   = os.getenv("NAMENODE_HOST", "namenode")


def hdfs_mkdir(path: str):
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", path],
        check=True, capture_output=True
    )


def hdfs_put_string(content: str, hdfs_path: str):
    """Write a string to an HDFS path using hdfs dfs -put from stdin."""
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-put", "-f", "-", hdfs_path],
        stdin=subprocess.PIPE
    )
    proc.communicate(content.encode("utf-8"))
    if proc.returncode != 0:
        raise RuntimeError(f"hdfs put failed for {hdfs_path}")


# ── Modes ─────────────────────────────────────────────────────────────────────
def run_stream_mode():
    """Open a TCP server socket and stream CSV records to Flink."""
    host = "0.0.0.0"
    port = int(os.getenv("SOCKET_PORT", "9999"))

    machines = [
        (f"MCH_{t}_{i:03d}", t)
        for t in MACHINE_TYPES
        for i in range(1, 4)   # 3 machines per type = 18 total
    ]
    op_hours = {mid: random.uniform(100, 5000) for mid, _ in machines}

    logger.info("Stream mode: listening on %s:%d", host, port)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(1)

    while True:
        logger.info("Waiting for Flink to connect…")
        conn, addr = srv.accept()
        logger.info("Flink connected from %s", addr)
        try:
            while True:
                for mid, mtype in machines:
                    op_hours[mid] += random.uniform(0.001, 0.01)
                    rec = make_record(mid, mtype, op_hours[mid], datetime.utcnow())
                    line = record_to_csv_line(rec) + "\n"
                    conn.sendall(line.encode("utf-8"))
                time.sleep(0.5)   # ~36 records / s across all machines
        except (BrokenPipeError, ConnectionResetError):
            logger.info("Flink disconnected, waiting for reconnect…")
        finally:
            conn.close()


def run_batch_mode(n_records: int = 500_000):
    """Generate a large CSV and upload to HDFS."""
    logger.info("Batch mode: generating %d records → HDFS %s", n_records, HDFS_INPUT)

    machines = [
        (f"MCH_{t}_{i:03d}", t)
        for t in MACHINE_TYPES
        for i in range(1, 11)   # 10 machines per type = 60 total
    ]
    op_hours = {mid: random.uniform(100, 9000) for mid, _ in machines}

    buf = io.StringIO()
    buf.write(",".join(COLUMNS) + "\n")   # header

    base_ts = datetime.utcnow() - timedelta(days=30)
    interval = timedelta(days=30) / n_records

    for i in range(n_records):
        mid, mtype = machines[i % len(machines)]
        op_hours[mid] += random.uniform(0.01, 0.1)
        ts  = base_ts + interval * i
        rec = make_record(mid, mtype, op_hours[mid], ts)
        buf.write(record_to_csv_line(rec) + "\n")

        if i % 50_000 == 0:
            logger.info("  generated %d / %d rows…", i, n_records)

    logger.info("Uploading CSV to HDFS…")
    hdfs_mkdir(HDFS_INPUT)
    filename = f"sensors_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    hdfs_put_string(buf.getvalue(), f"{HDFS_INPUT}/{filename}")
    logger.info("Done. HDFS path: %s/%s", HDFS_INPUT, filename)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode = os.getenv("MODE", "stream").lower()

    if mode == "stream":
        run_stream_mode()
    elif mode in ("batch", "hdfs"):
        n = int(os.getenv("BATCH_RECORDS", "500000"))
        run_batch_mode(n)
    else:
        logger.error("Unknown MODE=%s. Use 'stream' or 'batch'.", mode)
        sys.exit(1)
