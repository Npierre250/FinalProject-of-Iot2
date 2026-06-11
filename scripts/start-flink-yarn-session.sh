#!/bin/bash
# start-flink-yarn-session.sh
#
# This script is the heart of the "Flink runs inside the Hadoop cluster"
# requirement. It:
#   1. Waits for YARN ResourceManager to accept connections
#   2. Starts a Flink YARN *session* (not per-job mode)
#      → The session appears in YARN UI as a long-running RUNNING application
#      → Flink JobManager runs as a YARN ApplicationMaster
#      → Flink TaskManagers run as YARN containers on NodeManagers
#   3. Stores the YARN Application ID so jobs can be submitted to it
#   4. Submits the IoT streaming job into the live session
#
# The professor can then open:
#   http://localhost:8088  → see "Flink session on YARN" in RUNNING state
#   http://localhost:8088  → click the app → see TaskManagers and running jobs
#   http://localhost:9870  → browse /flink/checkpoints, /flink/output

set -e

FLINK_HOME=${FLINK_HOME:-/opt/flink}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}

# ── 1. Wait for YARN ResourceManager ─────────────────────────────────────────
echo "==> Waiting for YARN ResourceManager to be ready…"
until yarn --config "$HADOOP_CONF_DIR" node -list 2>/dev/null | grep -q "Total Nodes"; do
    echo "    YARN not ready yet, retrying in 5s…"
    sleep 5
done
echo "==> YARN is ready."
yarn --config "$HADOOP_CONF_DIR" node -list

# ── 2. Start Flink YARN Session ───────────────────────────────────────────────
# -n 2          → 2 TaskManagers (one per NodeManager)
# -s 2          → 2 slots per TaskManager  (total parallelism = 4)
# -jm 1600m     → JobManager memory
# -tm 1728m     → TaskManager memory
# -nm           → Application name shown in YARN UI
# -d            → Detached mode (script continues after session starts)
echo "==> Starting Flink YARN session…"
"$FLINK_HOME/bin/yarn-session.sh" \
    -n 2 \
    -s 2 \
    -jm 1600m \
    -tm 1728m \
    -nm "IoT-Sensor-Flink-Session" \
    -d

echo "==> Flink YARN session started."
echo "    Open http://localhost:8088 to see it in the YARN UI."

# ── 3. Wait for Flink session to be fully up ──────────────────────────────────
echo "==> Waiting for Flink REST endpoint to be reachable…"
FLINK_REST_PORT=8081
until yarn --config "$HADOOP_CONF_DIR" application -list 2>/dev/null | grep -q "IoT-Sensor-Flink-Session"; do
    echo "    Flink session not visible in YARN yet, retrying in 5s…"
    sleep 5
done

# Give the AM a few more seconds to fully initialise its REST endpoint
sleep 15
echo "==> Flink session is live in YARN."

# ── 4. Submit the IoT streaming job ──────────────────────────────────────────
# -py  → Python (PyFlink) job
# The job connects to data-generator-stream:9999 and writes to HDFS
echo "==> Submitting IoT streaming job to the Flink YARN session…"
"$FLINK_HOME/bin/flink" run \
    -py /flink-job/streaming_sensor_analyzer.py \
    -d

echo ""
echo "======================================================="
echo " Flink IoT pipeline is running inside Hadoop/YARN!"
echo ""
echo "  YARN UI  →  http://localhost:8088"
echo "  HDFS UI  →  http://localhost:9870"
echo ""
echo "  To submit the batch job manually:"
echo "    docker exec -it flink-client bash"
echo "    flink run -py /flink-job/batch_sensor_analyzer.py"
echo ""
echo "  To read streaming output from HDFS:"
echo "    docker exec namenode hdfs dfs -ls /flink/output/streaming/"
echo "======================================================="

# Keep container alive so logs remain accessible
tail -f /dev/null
