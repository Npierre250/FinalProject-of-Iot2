# Flink Sensor Processing — Hadoop Edition v2
## Apache Flink on YARN + HDFS · IoT Factory Sensor Pipeline

```
IoT Sensors → data-generator-stream:9999 (TCP)
                        │
              Flink streaming job (submitted to YARN session)
                        │
              Flink JobManager  ←── runs as YARN ApplicationMaster
              Flink TaskManagers ←── run as YARN containers
                        │
                      HDFS
              /flink/output/streaming/all/
              /flink/output/streaming/critical/
              /flink/checkpoints/
```

---

## Proof that Flink runs INSIDE the Hadoop cluster

Open **http://localhost:8088** after startup. You will see:

```
Application Name          State    FinalState   Type
IoT-Sensor-Flink-Session  RUNNING  UNDEFINED    Apache Flink
```

Click the app → you see the Flink Web UI hosted from inside YARN, showing:
- The JobManager running as an ApplicationMaster on a NodeManager
- 2 TaskManagers allocated by YARN
- The running "IoT Streaming Analyzer" job with its DAG

This is the university requirement: **Flink runs inside the Hadoop cluster**.

---

## Architecture

| Container | Image | Role | Web UI |
|---|---|---|---|
| `namenode` | apache/hadoop:3 | HDFS metadata | http://localhost:9870 |
| `datanode1/2` | apache/hadoop:3 | HDFS block storage | — |
| `resourcemanager` | apache/hadoop:3 | YARN scheduler | http://localhost:8088 |
| `nodemanager1/2` | apache/hadoop:3 | YARN compute | http://localhost:8042 |
| `flink-client` | flink:1.18.1 + Hadoop | Starts session, submits jobs | — |
| `data-generator-stream` | python:3.9 + Hadoop | TCP socket → Flink | — |
| `data-generator-batch` | python:3.9 + Hadoop | CSV → HDFS | — |

---

## Requirements

- Docker Desktop with **14 GB RAM** (12 GB minimum)
- Docker Compose v2
- ~15 GB free disk space

---

## Start everything (one command)

```bash
docker compose up -d --build
```

The `flink-client` container will automatically:
1. Wait for YARN to be ready
2. Start a Flink YARN session (visible in YARN UI as a RUNNING application)
3. Submit the IoT streaming job into that session

Watch progress:

```bash
docker compose logs -f flink-client
```

Expected output:
```
==> YARN is ready.
==> Starting Flink YARN session…
==> Flink YARN session started.
==> Submitting IoT streaming job to the Flink YARN session…
==> Flink IoT pipeline is running inside Hadoop/YARN!
```

---

## Verify the pipeline

```bash
# 1. Check YARN sees 2 NodeManagers
docker exec resourcemanager yarn node -list

# 2. Check Flink session is in YARN
docker exec resourcemanager yarn application -list

# 3. Check HDFS has received data
docker exec namenode hdfs dfs -ls /flink/output/streaming/

# 4. Read critical anomaly records
docker exec namenode hdfs dfs -cat "/flink/output/streaming/critical/*"

# 5. Check checkpoints are on HDFS
docker exec namenode hdfs dfs -ls /flink/checkpoints/
```

---

## Run the batch job

```bash
# Generate batch data (500,000 records → HDFS /flink/input/)
docker compose run --rm data-generator-batch

# Submit batch analysis job
docker exec -it flink-client bash
flink run -py /flink-job/batch_sensor_analyzer.py

# Read results
docker exec namenode hdfs dfs -cat "/flink/output/batch/*"
```

---

## HDFS directory layout

```
hdfs://namenode:9000/
└── flink/
    ├── checkpoints/          ← Flink exactly-once state (30s interval)
    ├── savepoints/           ← Manual savepoints
    ├── input/                ← Batch CSV files
    └── output/
        ├── streaming/
        │   ├── all/          ← All records (JSON lines)
        │   └── critical/     ← severity ≥ 2 records
        └── batch/            ← Per-machine-type aggregates
```

---

## Key changes from v1

| Issue | v1 | v2 (fixed) |
|---|---|---|
| Flink on YARN | `flink run -m yarn-cluster` (per-job, temporary) | `yarn-session.sh` (persistent session, always visible in YARN UI) |
| Auto-start | Manual — exec into container first | Automatic on `docker compose up` |
| Port conflict | Generator on 9000 = HDFS RPC collision | Generator on **9999** |
| Docker images | `wget` Hadoop/Flink tarballs at build time | **Official Docker Hub images** (`apache/hadoop:3`, `flink:1.18.1`) |
| PyFlink API | `FileSink` / `RollingPolicy` (version-sensitive) | `write_as_text()` (stable across 1.16–1.18) |

---

## Useful commands

```bash
# Stream logs from the Flink client (shows session start + job submission)
docker compose logs -f flink-client

# Kill and restart the Flink YARN session
docker compose restart flink-client

# Check all container statuses
docker compose ps

# Wipe everything including HDFS volumes
docker compose down -v
```
