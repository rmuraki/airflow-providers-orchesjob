# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Ryosuke Muraki
"""Tests for OrchesJobSensor."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow_providers_orchesjob.sensors.orchesjob import OrchesJobSensor


def _make_sensor(**kwargs) -> OrchesJobSensor:
    defaults = dict(
        task_id="test_sensor",
        job_id="abc-123",
        ssh_conn_id="my_ssh",
    )
    defaults.update(kwargs)
    return OrchesJobSensor(**defaults)


def _make_ssh_client(stdout_data: str, exit_status: int = 0):
    stdout = MagicMock()
    stdout.read.return_value = stdout_data.encode()
    stdout.channel.recv_exit_status.return_value = exit_status
    stderr = MagicMock()
    stderr.read.return_value = b""
    client = MagicMock()
    client.exec_command.return_value = (None, stdout, stderr)
    return client


class TestPoke:
    def test_running_returns_false(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "RUNNING"}):
            assert sensor.poke({}) is False

    def test_starting_returns_false(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "STARTING"}):
            assert sensor.poke({}) is False

    def test_succeeded_returns_true(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "SUCCEEDED", "exit_code": 0}):
            assert sensor.poke({}) is True

    def test_failed_raises(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "FAILED", "exit_code": 1}):
            with pytest.raises(AirflowException, match="FAILED"):
                sensor.poke({})

    def test_lost_raises(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "LOST", "exit_code": None}):
            with pytest.raises(AirflowException, match="LOST"):
                sensor.poke({})

    def test_cancelled_raises(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "CANCELLED", "exit_code": None}):
            with pytest.raises(AirflowException, match="CANCELLED"):
                sensor.poke({})

    def test_unknown_status_returns_false(self):
        sensor = _make_sensor()
        with patch.object(sensor, "_ssh_status", return_value={"status": "UNKNOWN"}):
            assert sensor.poke({}) is False


class TestSshStatus:
    def test_success(self):
        sensor = _make_sensor()
        payload = {"status": "RUNNING"}
        client = _make_ssh_client(json.dumps(payload))
        with patch("airflow_providers_orchesjob.sensors.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            result = sensor._ssh_status()
        assert result["status"] == "RUNNING"

    def test_job_id_in_command(self):
        sensor = _make_sensor(job_id="my-job-999")
        client = _make_ssh_client(json.dumps({"status": "RUNNING"}))
        with patch("airflow_providers_orchesjob.sensors.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            sensor._ssh_status()
        cmd = client.exec_command.call_args[0][0]
        assert "my-job-999" in cmd

    def test_non_zero_exit_raises(self):
        sensor = _make_sensor()
        client = _make_ssh_client("", exit_status=1)
        with patch("airflow_providers_orchesjob.sensors.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            with pytest.raises(AirflowException, match="orchesjob status failed"):
                sensor._ssh_status()

    def test_non_json_raises(self):
        sensor = _make_sensor()
        client = _make_ssh_client("not-json")
        with patch("airflow_providers_orchesjob.sensors.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            with pytest.raises(AirflowException, match="non-JSON"):
                sensor._ssh_status()

    def test_orchesjob_home_prepended(self):
        sensor = _make_sensor(orchesjob_home="/opt/orchesjob")
        client = _make_ssh_client(json.dumps({"status": "RUNNING"}))
        with patch("airflow_providers_orchesjob.sensors.orchesjob.SSHHook") as MockHook:
            MockHook.return_value.get_conn.return_value = client
            sensor._ssh_status()
        cmd = client.exec_command.call_args[0][0]
        assert cmd.startswith("ORCHESJOB_HOME=/opt/orchesjob")


class TestBuildCmd:
    def test_no_home(self):
        sensor = _make_sensor()
        cmd = sensor._build_cmd(["orchesjob", "status", "--job-id", "abc"])
        assert cmd == "orchesjob status --job-id abc"

    def test_with_home(self):
        sensor = _make_sensor(orchesjob_home="/opt/oj")
        cmd = sensor._build_cmd(["orchesjob", "status"])
        assert cmd.startswith("ORCHESJOB_HOME=/opt/oj")

    def test_home_with_spaces_quoted(self):
        sensor = _make_sensor(orchesjob_home="/path with spaces")
        cmd = sensor._build_cmd(["orchesjob", "status"])
        assert "'/path with spaces'" in cmd
