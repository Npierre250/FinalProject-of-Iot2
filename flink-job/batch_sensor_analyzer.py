#!/usr/bin/env python3
"""
batch_sensor_analyzer.py  (v2 — YARN + HDFS)
Historical batch analysis running on Flink inside YARN.
Reads CSV from HDFS /flink/input/, computes per-machine-type aggregates,
writes JSON results to HDFS /flink/output/batch/.
"""

import os
import json
import logging
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, FilterFunction, ReduceFunction
from pyflink.common import Types
from pyflink.common.enums import WriteMode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TEMP_THRESHOLD      = 75.0
VIBRATION_THRESHOLD = 20.0
SOUND_THRESHOLD     = 80.0

HDFS_BASE   = os.getenv("HDFS_BASE",   "hdfs://namenode:9000")
HDFS_INPUT  = f"{HDFS_BASE}/flink/input"
HDFS_OUTPUT = f"{HDFS_BASE}/flink/output/batch"

COLUMNS = [
    "machine_id", "machine_type", "operating_hours",
    "temperature_c", "vibration_mm_s", "sound_db",
    "power_kw", "maintenance_required", "error_count",
    "last_maintenance_days", "production_rate", "fault_detected",
    "timestamp"
]


class ParseCsv(MapFunction):
    def map(self, line: str):
        try:
            line = line.strip()
            if not line or line.startswith("machine_id"):
                return None
            parts = line.split(",")
            if len(parts) < len(COLUMNS):
                return None
            r = dict(zip(COLUMNS, parts))
            r["temperature_c"]   = float(r["temperature_c"])
            r["vibration_mm_s"]  = float(r["vibration_mm_s"])
            r["sound_db"]        = float(r["sound_db"])
            r["power_kw"]        = float(r["power_kw"])
            r["error_count"]     = int(r["error_count"])
            r["operating_hours"] = float(r["operating_hours"])
            return r
        except Exception:
            return None


class DropNone(FilterFunction):
    def filter(self, v):
        return v is not None


class ToAccumulator(MapFunction):
    """Convert a record to an accumulator dict keyed by machine_type."""
    def map(self, r):
        fault = str(r.get("fault_detected", "")).lower() in ("true", "1", "yes")
        return {
            "machine_type":    r["machine_type"],
            "count":           1,
            "sum_temp":        r["temperature_c"],
            "sum_vib":         r["vibration_mm_s"],
            "sum_sound":       r["sound_db"],
            "sum_power":       r["power_kw"],
            "sum_errors":      r["error_count"],
            "above_temp":      1 if r["temperature_c"]  > TEMP_THRESHOLD      else 0,
            "above_vib":       1 if r["vibration_mm_s"] > VIBRATION_THRESHOLD else 0,
            "above_sound":     1 if r["sound_db"]       > SOUND_THRESHOLD     else 0,
            "fault_count":     1 if fault else 0,
        }


class MergeAccumulators(ReduceFunction):
    def reduce(self, a, b):
        return {
            "machine_type": a["machine_type"],
            "count":        a["count"]      + b["count"],
            "sum_temp":     a["sum_temp"]   + b["sum_temp"],
            "sum_vib":      a["sum_vib"]    + b["sum_vib"],
            "sum_sound":    a["sum_sound"]  + b["sum_sound"],
            "sum_power":    a["sum_power"]  + b["sum_power"],
            "sum_errors":   a["sum_errors"] + b["sum_errors"],
            "above_temp":   a["above_temp"] + b["above_temp"],
            "above_vib":    a["above_vib"]  + b["above_vib"],
            "above_sound":  a["above_sound"]+ b["above_sound"],
            "fault_count":  a["fault_count"]+ b["fault_count"],
        }


class ToSummaryJson(MapFunction):
    def map(self, agg) -> str:
        n = max(agg["count"], 1)
        summary = {
            "machine_type":              agg["machine_type"],
            "total_readings":            n,
            "avg_temperature_c":         round(agg["sum_temp"]   / n, 2),
            "avg_vibration_mm_s":        round(agg["sum_vib"]    / n, 2),
            "avg_sound_db":              round(agg["sum_sound"]  / n, 2),
            "avg_power_kw":              round(agg["sum_power"]  / n, 2),
            "total_errors":              agg["sum_errors"],
            "pct_above_temp":            round(agg["above_temp"]  / n * 100, 1),
            "pct_above_vibration":       round(agg["above_vib"]   / n * 100, 1),
            "pct_above_sound":           round(agg["above_sound"] / n * 100, 1),
            "fault_rate_pct":            round(agg["fault_count"] / n * 100, 1),
            "health_status": (
                "CRITICAL" if agg["fault_count"] / n > 0.30 else
                "WARNING"  if agg["fault_count"] / n > 0.10 else
                "OK"
            ),
            "computed_at": datetime.utcnow().isoformat(),
        }
        return json.dumps(summary)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    logger.info("Reading CSV files from HDFS: %s", HDFS_INPUT)

    # Read all CSV files from HDFS input directory
    raw = env.read_text_file(HDFS_INPUT)

    aggregated = (
        raw
        .map(ParseCsv(),        output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(DropNone())
        .map(ToAccumulator(),   output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda a: a["machine_type"])
        .reduce(MergeAccumulators())
        .map(ToSummaryJson(),   output_type=Types.STRING())
    )

    aggregated.write_as_text(HDFS_OUTPUT, write_mode=WriteMode.OVERWRITE)

    logger.info("Submitting batch job to Flink YARN session…")
    env.execute("IoT Batch Analyzer — HDFS")


if __name__ == "__main__":
    main()
