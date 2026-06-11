#!/bin/bash
# entrypoint-nodemanager.sh
set -e

echo "==> Starting YARN NodeManager (hostname: $(hostname))…"
yarn nodemanager
