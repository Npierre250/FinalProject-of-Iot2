#!/usr/bin/env python3
"""
streaming_sensor_analyzer.py  (v2 — YARN + HDFS)
Real-time IoT sensor anomaly detection running on Flink inside YARN.

Pipeline: data-generator-stream:9999 (TCP) → Flink (YARN session) → HDFS

Port 9999 is used (not 9000) to avoid collision with the HDFS RPC port.
"""

import os
import json
import logging
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.common import Types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Thresholds (original project values) ──────────────────────────────────────
TEMP_THRESHOLD      = 75.0
VIBRATION_THRESHOLD = 20.0
SOUND_THRESHOLD     = 80.0

# ── Environment variables ──────────────────────────────────────────────────────
HDFS_BASE    = os.getenv("HDFS_BASE",    "hdfs://namenode:9000")
SOCKET_HOST  = os.getenv("SOCKET_HOST",  "data-generator-stream")
SOCKET_PORT  = int(os.getenv("SOCKET_PORT", "9999"))   # NOT 9000

HDFS_OUT_ALL      = f"{HDFS_BASE}/flink/output/streaming/all"
HDFS_OUT_CRITICAL = f"{HDFS_BASE}/flink/output/streaming/critical"

COLUMNS = [
    "machine_id", "machine_type", "operating_hours",
    "temperature_c", "vibration_mm_s", "sound_db",
    "power_kw", "maintenance_required", "error_count",
    "last_maintenance_days", "production_rate", "fault_detected",
    "timestamp"
]


class ParseRecord(MapFunction):
    def map(self, line: str):
        try:
            parts = line.strip().split(",")
            if len(parts) < len(COLUMNS):
                return None
            r = dict(zip(COLUMNS, parts))
            r["temperature_c"]   = float(r["temperature_c"])
            r["vibration_mm_s"]  = float(r["vibration_mm_s"])
            r["sound_db"]        = float(r["sound_db"])
            r["power_kw"]        = float(r["power_kw"])
            r["error_count"]     = int(r["error_count"])
            r["operating_hours"] = float(r["operating_hours"])
            r["ingested_at"]     = datetime.utcnow().isoformat()
            return r
        except Exception:
            return None


class DropNone(FilterFunction):
    def filter(self, v):
        return v is not None


class EnrichWithAnomalies(MapFunction):
    def map(self, r: dict):
        anomalies = []
        if r["temperature_c"]  > TEMP_THRESHOLD:
            anomalies.append(f"HIGH_TEMP({r['temperature_c']:.1f}C)")
        if r["vibration_mm_s"] > VIBRATION_THRESHOLD:
            anomalies.append(f"HIGH_VIB({r['vibration_mm_s']:.1f}mm/s)")
        if r["sound_db"]       > SOUND_THRESHOLD:
            anomalies.append(f"HIGH_SOUND({r['sound_db']:.1f}dB)")
        r["anomalies"]     = anomalies
        r["severity"]      = len(anomalies)
        r["is_critical"]   = len(anomalies) >= 2
        r["processed_at"]  = datetime.utcnow().isoformat()
        return r


class ToJson(MapFunction):
    def map(self, r) -> str:
        return json.dumps(r)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Checkpoints stored on HDFS
    env.enable_checkpointing(30_000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)

    # Source: TCP socket from the IoT generator (port 9999)
    logger.info("Connecting to %s:%d", SOCKET_HOST, SOCKET_PORT)
    raw = env.socket_text_stream(SOCKET_HOST, SOCKET_PORT)

    # Processing
    parsed = (
        raw
        .map(ParseRecord(),         output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(DropNone())
        .map(EnrichWithAnomalies(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # All records → HDFS (plain text sink — compatible with all PyFlink 1.18 builds)
    all_json = parsed.map(ToJson(), output_type=Types.STRING())
    all_json.write_as_text(HDFS_OUT_ALL, write_mode=
        __import__("pyflink.common.enums", fromlist=["WriteMode"]).WriteMode.OVERWRITE
    )

    # Critical anomalies → separate HDFS path
    critical_json = (
        parsed
        .filter(lambda r: r.get("is_critical", False))
        .map(ToJson(), output_type=Types.STRING())
    )
    critical_json.write_as_text(HDFS_OUT_CRITICAL, write_mode=
        __import__("pyflink.common.enums", fromlist=["WriteMode"]).WriteMode.OVERWRITE
    )

    logger.info("Submitting streaming job to Flink YARN session…")
    env.execute("IoT Streaming Analyzer — HDFS")


if __name__ == "__main__":
    main()
