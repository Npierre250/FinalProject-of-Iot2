#!/bin/bash
# entrypoint-datanode.sh
set -e

echo "==> Starting DataNode (hostname: $(hostname))…"
hdfs datanode
