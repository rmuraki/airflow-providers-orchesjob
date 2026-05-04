# airflow-providers-orchesjob

> ⚠️ **EXPERIMENTAL**
>
> This package is an experimental implementation.
> **Do not use in production.** APIs and behaviour may change without notice.

Apache Airflow provider for [orchesjob](https://github.com/your-org/orchesjob).
Starts and monitors orchesjob jobs on a remote host over SSH.

---

## ⚠️ Known Limitations

### `mode="reschedule"` is required

**You must use `OrchesJobSensor` with `mode="reschedule"`. The default `mode="poke"` does not work.**

```python
# ❌ BROKEN: poke mode (default) — do not use
OrchesJobSensor(
    task_id="wait",
    job_id="...",
    ssh_conn_id="my_ssh",
    poke_interval=30.0,
)

# ✅ CORRECT: always specify mode="reschedule"
OrchesJobSensor(
    task_id="wait",
    job_id="...",
    ssh_conn_id="my_ssh",
    poke_interval=30.0,
    mode="reschedule",  # required
)
```

In `poke` mode the worker process stays alive for the entire duration of the sensor.
In environments such as MWAA, the Airflow server responds with `Task Instance not found`
for tasks that remain in `running` state for a long time, causing the worker to forcibly
terminate itself.

In `reschedule` mode the worker exits normally after each `False` return from `poke()`,
and the scheduler re-queues the task after `poke_interval` seconds, avoiding this problem.

---

## Requirements

- Apache Airflow ≥ 2.6
- `apache-airflow-providers-ssh` ≥ 3.0
- `orchesjob` installed on the remote host
- `orchesjob-reserver` installed on the remote host (only when using `OrchesJobReserveOperator`)

## Installation

```bash
pip install airflow-providers-orchesjob
```

## Setup

Register an SSH Connection in Airflow (Admin → Connections):

| Field | Value |
|-------|-------|
| Conn Id | any name (e.g. `my_ssh`) |
| Conn Type | SSH |
| Host | remote host address |
| Username | SSH username |

## Usage

Use `OrchesJobOperator` to start a job and `OrchesJobSensor` to wait for completion.

```python
from airflow.decorators import dag
from airflow_providers_orchesjob.operators.orchesjob import OrchesJobOperator
from airflow_providers_orchesjob.sensors.orchesjob import OrchesJobSensor

@dag(dag_id="my_dag", ...)
def my_dag():
    start = OrchesJobOperator(
        task_id="run_job",
        command=["/jobs/import.sh", "--date", "{{ ds }}"],
        ssh_conn_id="my_ssh",
    )

    wait = OrchesJobSensor(
        task_id="wait_job",
        job_id="{{ ti.xcom_pull(task_ids='run_job', key='job_id') }}",
        ssh_conn_id="my_ssh",
        poke_interval=30.0,
        timeout=3600.0,
        mode="reschedule",  # required
    )

    start >> wait
```

### Reserving a job with OrchesJobReserveOperator

Use `OrchesJobReserveOperator` to register a job reservation via `orchesjob-reserver` on the remote host.
A reservation schedules a job for future execution with optional timing constraints (`not_before`, `expires_at`).

The operator returns the reservation payload as an XCom value.

```python
from airflow.decorators import dag
from airflow_providers_orchesjob.operators.reserver import OrchesJobReserveOperator

@dag(dag_id="my_dag", ...)
def my_dag():
    reserve = OrchesJobReserveOperator(
        task_id="reserve_job",
        ssh_conn_id="my_ssh",
        run_key="daily-import-{{ ds }}",
        command=["/jobs/import.sh", "--date", "{{ ds }}"],
        orchesjob_home="/path/to/orchesjob",
        not_before="{{ ds }}T02:00:00+09:00",   # earliest start time (optional)
        expires_at="{{ ds }}T06:00:00+09:00",   # reservation expiry (optional)
        metadata={"env": "prod"},               # arbitrary metadata (optional)
    )
```

### Idempotency

`run_key` defaults to `{dag_id}__{task_id}__{run_id}`.
Re-triggering the same DAG run will not re-execute the job if it is still active.

```python
# Explicit run_key
OrchesJobOperator(
    task_id="import",
    command=["/jobs/import.sh"],
    ssh_conn_id="my_ssh",
    run_key="daily-import-{{ ds }}",
)
```

Set `strict=True` to prevent any re-execution with the same `run_key`,
even after the previous job has finished.

## Parameters

### OrchesJobReserveOperator

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ssh_conn_id` | `str` | **required** | Airflow SSH Connection ID |
| `run_key` | `str` | **required** | orchesjob-reserver idempotency key |
| `command` | `list[str]` | **required** | Command to run on the remote host |
| `not_before` | `str \| None` | `None` | Earliest time the job may start (ISO 8601) |
| `expires_at` | `str \| None` | `None` | Time after which the reservation expires (ISO 8601) |
| `metadata` | `dict \| None` | `None` | Arbitrary metadata attached to the reservation |
| `metadata_json` | `str \| None` | `None` | Pre-serialised JSON metadata (takes precedence over `metadata`) |
| `orchesjob_home` | `str \| None` | `None` | Override `ORCHESJOB_HOME` on the remote host |
| `orchesjob_reserver_bin` | `str` | `"orchesjob-reserver"` | Path to the `orchesjob-reserver` binary on the remote host |
| `orchesjob_start_options` | `list[str] \| None` | `None` | Extra options forwarded to `orchesjob start` |
| `remote_db` | `str \| None` | `None` | Override the orchesjob-reserver database path on the remote host |
| `remote_orchesjob_bin` | `str \| None` | `None` | Override the `orchesjob` binary path on the remote host |
| `command_timeout` | `int` | `30` | SSH command timeout in seconds |
| `env` | `dict[str, str] \| None` | `None` | Extra environment variables set before the command |

### OrchesJobOperator

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `command` | `list[str]` | **required** | Command to run on the remote host |
| `ssh_conn_id` | `str` | **required** | Airflow SSH Connection ID |
| `run_key` | `str \| None` | auto | orchesjob idempotency key |
| `strict` | `bool` | `False` | Prevent re-execution with the same `run_key` |
| `orchesjob_home` | `str \| None` | `None` | Override `ORCHESJOB_HOME` on the remote host |

### OrchesJobSensor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `job_id` | `str` | **required** | orchesjob job ID to monitor |
| `ssh_conn_id` | `str` | **required** | Airflow SSH Connection ID |
| `orchesjob_home` | `str \| None` | `None` | Override `ORCHESJOB_HOME` on the remote host |
| `poke_interval` | `float` | `30.0` | Seconds between polls |
| `timeout` | `float` | `3600.0` | Sensor timeout in seconds |
| `mode` | `str` | `"poke"` | **Must be set to `"reschedule"`** |

## Error Handling

| Event | Airflow behaviour |
|-------|------------------|
| Job `FAILED` or `LOST` | `AirflowException` → task `retries` apply |
| Job `CANCELLED` | `AirflowException` |
| SSH connection error | `AirflowException` → task `retries` apply |
| Sensor `timeout` exceeded | `AirflowSensorTimeout` → task `retries` apply |

## License

MIT
