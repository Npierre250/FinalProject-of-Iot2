#!/bin/bash
set -e

FLINK_HOME=${FLINK_HOME:-/opt/flink}
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}
HADOOP_YARN="$HADOOP_HOME/bin/yarn"

# ── 1. Wait for YARN ResourceManager ─────────────────────────────────────────
echo "==> Waiting for YARN ResourceManager to be ready…"
until "$HADOOP_YARN" --config "$HADOOP_CONF_DIR" node -list 2>/dev/null | grep -q "Total Nodes"; do
    echo "    YARN not ready yet, retrying in 5s…"
    sleep 5
done
echo "==> YARN is ready."
"$HADOOP_YARN" --config "$HADOOP_CONF_DIR" node -list

# ── 2. Start Flink YARN Session ───────────────────────────────────────────────
echo "==> Starting Flink YARN session…"
export HADOOP_CLASSPATH=$("$HADOOP_HOME/bin/hadoop" classpath 2>/dev/null)
"$FLINK_HOME/bin/yarn-session.sh" \
    -n 2 \
    -s 2 \
    -jm 1600m \
    -tm 1728m \
    -nm "IoT-Sensor-Flink-Session" \
    -d

echo "==> Flink YARN session started."
echo "    Open http://localhost:8088 to see it in the YARN UI."

# ── 3. Wait for Flink session to appear in YARN ──────────────────────────────
echo "==> Waiting for Flink session to appear in YARN…"
until "$HADOOP_YARN" --config "$HADOOP_CONF_DIR" application -list 2>/dev/null | grep -q "IoT-Sensor-Flink-Session"; do
    echo "    Flink session not visible in YARN yet, retrying in 5s…"
    sleep 5
done
sleep 15
echo "==> Flink session is live in YARN."

# ── 4. Submit the IoT streaming job ──────────────────────────────────────────
echo "==> Submitting IoT streaming job to the Flink YARN session…"
export HADOOP_CLASSPATH=$("$HADOOP_HOME/bin/hadoop" classpath 2>/dev/null)
"$FLINK_HOME/bin/flink" run \
    -py /flink-job/streaming_sensor_analyzer.py \
    -d

echo ""
echo "======================================================="
echo " Flink IoT pipeline is running inside Hadoop/YARN!"
echo "  YARN UI  →  http://localhost:8088"
echo "  HDFS UI  →  http://localhost:9870"
echo "======================================================="

tail -f /dev/null