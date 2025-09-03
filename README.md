# Simple Airflow Lab (lab-2)

A minimal, single-node Airflow setup using dag-factory for declarative DAG creation.

## What's Different from lab-1?

- **Single Node**: No Redis, no Celery workers, just LocalExecutor
- **Minimal Services**: Only Postgres + Airflow
- **Simple DAG**: One basic "Hello World" DAG using dag-factory
- **Easy Setup**: Just 2 commands to get running

## Quick Start

1. **Start Airflow**:
   ```bash
   docker-compose up -d
   ```

2. **Access Airflow UI**:
   - URL: http://localhost:8081
   - Username: `admin`
   - Password: `admin`

3. **View Your DAG**:
   - Look for "hello_world_dag" in the Airflow UI
   - It runs daily and has 3 simple tasks in sequence

## Architecture

```
┌─────────────┐    ┌──────────────┐
│  Postgres   │◄───│   Airflow    │
│  Database   │    │ (Web+Sched)  │
└─────────────┘    └──────────────┘
                          │
                          ▼
                   ┌──────────────┐
                   │  Simple DAG  │
                   │ (dag-factory) │
                   └──────────────┘
```
