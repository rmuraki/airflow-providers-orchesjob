# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Ryosuke Muraki
"""Tests for OrchesJobOperator."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow_providers_orchesjob.operators.orchesjob import OrchesJobOperator


def _make_operator(**kwargs) -> OrchesJobOperator:
    defaults = dict(
        task_id="test_task",
        command=["/jobs/import.sh"],
        ssh_conn_id="my_ssh",
    )
    defaults.update(kwargs)
    return OrchesJobOperator(**defaults)


def _make_context(dag_id="my_dag", task_id="my_task", run_id="manual__2026-01-01"):
    dag = MagicMock()
    dag.dag_id = dag_id
    task = MagicMock()
    task.task_id = task_id
    ti = MagicMock()
    return {"dag": dag, "task": task, "run_id": run_id, "ti": ti}


_START_RESPONSE = {
    "accepted": True,
    "existing": False,
    "job_id": "abc-123",
    "status": "RUNNING",
    "run_key": "test-key",
    "strict": False,
    "command": ["/jobs/import.sh"],
    "pid": 42,
    "stdout_file": "/var/lib/orchesjob/logs/abc-123.stdout",
    "stderr_file": "/var/lib/orchesjob/logs/abc-123.stderr",
    "exit_code": None,
    "started_at": "2026-01-01T00:00:00Z",
    "finished_at": None,
}


class TestResolveRunKey:
    def test_explicit_run_key(self):
        op = _make_operator(run_key="my-key")
        assert op._resolve_run_key(_make_context()) == "my-key"

    def test_auto_run_key(self):
        op = _make_operator()
        ctx = _make_context(dag_id="d", task_id="t", run_id="r")
        assert op._resolve_run_key(ctx) == "d__t__r"


class TestExecute:
    def test_returns_job_id(self):
        op = _make_operator()
        ctx = _make_context()
        with patch.object(op, "_ssh_start", return_value=_START_RESPONSE):
            result = op.execute(ctx)
        assert result == "abc-123"

    def test_pushes_xcom(self):
        op = _make_operator()
        ctx = _make_context()
        with patch.object(op, "_ssh_start", return_value=_START_RESPONSE):
            op.execute(ctx)
        ctx["ti"].xcom_push.assert_called_once_with(key="job_id", value="abc-123")

    def test_ssh_failure_raises(self):
        op = _make_operator()
        with patch.object(op, "_ssh_start", side_effect=AirflowException("SSH failed")):
            with pytest.raises(AirflowException, match="SSH failed"):
                op.execute(_make_context())


class TestSshStart:
    def _make_ssh_client(self, stdout_data: str, exit_status: int = 0):
        stdout = MagicMock()
        stdout.read.return_value = stdout_data.encode()
        stdout.channel.recv_exit_status.return_value = exit_status
        stderr = MagicMock()
        stderr.read.return_value = b""
        client = MagicMock()
        client.exec_command.return_value = (None, stdout, stderr)
        return client

    def test_success(self):
        op = _make_operator()
        client = self._make_ssh_client(json.dumps(_START_RESPONSE))
        with patch("airflow_providers_orchesjob.operators.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            result = op._ssh_start("my-key")
        assert result["job_id"] == "abc-123"

    def test_non_zero_exit_raises(self):
        op = _make_operator()
        client = self._make_ssh_client("", exit_status=1)
        with patch("airflow_providers_orchesjob.operators.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            with pytest.raises(AirflowException, match="orchesjob start failed"):
                op._ssh_start("my-key")

    def test_non_json_raises(self):
        op = _make_operator()
        client = self._make_ssh_client("not-json")
        with patch("airflow_providers_orchesjob.operators.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            with pytest.raises(AirflowException, match="non-JSON"):
                op._ssh_start("my-key")

    def test_strict_flag_in_command(self):
        op = _make_operator(strict=True)
        client = self._make_ssh_client(json.dumps(_START_RESPONSE))
        with patch("airflow_providers_orchesjob.operators.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            op._ssh_start("my-key")
        cmd = client.exec_command.call_args[0][0]
        assert "--strict" in cmd

    def test_orchesjob_home_prepended(self):
        op = _make_operator(orchesjob_home="/custom/home")
        client = self._make_ssh_client(json.dumps(_START_RESPONSE))
        with patch("airflow_providers_orchesjob.operators.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            op._ssh_start("my-key")
        cmd = client.exec_command.call_args[0][0]
        assert cmd.startswith("ORCHESJOB_HOME=/custom/home")


class TestBuildCmd:
    def test_no_home(self):
        op = _make_operator()
        cmd = op._build_cmd(["orchesjob", "start", "--", "echo", "hello"])
        assert cmd == "orchesjob start -- echo hello"

    def test_with_home(self):
        op = _make_operator(orchesjob_home="/opt/orchesjob")
        cmd = op._build_cmd(["orchesjob", "start"])
        assert cmd.startswith("ORCHESJOB_HOME=/opt/orchesjob")

    def test_home_with_spaces_quoted(self):
        op = _make_operator(orchesjob_home="/path with spaces")
        cmd = op._build_cmd(["orchesjob", "start"])
        assert "'/path with spaces'" in cmd

    def test_args_with_spaces_quoted(self):
        op = _make_operator()
        cmd = op._build_cmd(["orchesjob", "start", "--", "echo", "hello world"])
        assert "'hello world'" in cmd
