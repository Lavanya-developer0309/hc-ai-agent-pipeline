#!/bin/bash
# Start Airflow on this Mac:  ./run_airflow.sh   ->  http://localhost:8080
# Login: user "admin", password in airflow_home/simple_auth_manager_passwords.json.generated
cd "$(dirname "$0")"
export AIRFLOW_HOME="$PWD/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT=8080
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Kolkata
# one batch at a time on a 16 GB Mac that also runs the AI model: few workers, patient health checks
export AIRFLOW__CORE__PARALLELISM=2
export AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT=1200
# macOS: forked task processes crash in some network libraries without these
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy="*"
export PATH="$PWD/.venv/bin:$PATH"   # standalone starts its parts as separate "airflow" commands
exec airflow standalone
