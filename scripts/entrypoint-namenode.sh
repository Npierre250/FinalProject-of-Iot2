#!/bin/bash
# entrypoint-namenode.sh
# Format HDFS on first boot, then start NameNode.
set -e

NAMENODE_DIR=/hadoop/dfs/name

if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "==> Formatting HDFS NameNode (first boot)…"
    hdfs namenode -format -force -nonInteractive
    echo "==> Format complete."
fi

echo "==> Starting NameNode…"
hdfs namenode &
NAMENODE_PID=$!

# Wait for NameNode to be ready
echo "==> Waiting for NameNode to come online…"
until hdfs dfsadmin -report &>/dev/null; do
    sleep 3
done

# Create required HDFS directories
echo "==> Creating HDFS directory structure…"
hdfs dfs -mkdir -p /flink/checkpoints
hdfs dfs -mkdir -p /flink/savepoints
hdfs dfs -mkdir -p /flink/input
hdfs dfs -mkdir -p /flink/output/streaming/all
hdfs dfs -mkdir -p /flink/output/streaming/critical
hdfs dfs -mkdir -p /flink/output/batch
hdfs dfs -mkdir -p /yarn/logs
hdfs dfs -chmod -R 777 /flink
hdfs dfs -chmod -R 777 /yarn
echo "==> HDFS directories ready."

wait $NAMENODE_PID
