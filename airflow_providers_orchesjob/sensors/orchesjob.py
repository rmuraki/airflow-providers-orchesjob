# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Ryosuke Muraki
"""Airflow sensor that polls orchesjob job status over SSH."""

from __future__ import annotations

import json
import shlex
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "LOST", "CANCELLED"}


class OrchesJobSensor(BaseSensorOperator):
    """Poll ``orchesjob status`` over SSH until the job reaches a terminal state.

    Raises :class:`~airflow.exceptions.AirflowException` if the job ends with
    any status other than ``SUCCEEDED``.

    Parameters
    ----------
    job_id:
        orchesjob job ID to monitor.  Supports Jinja templating so you can
        pass ``"{{ ti.xcom_pull(task_ids='start_job', key='job_id') }}"``.
    ssh_conn_id:
        Airflow SSH Connection ID.
    orchesjob_home:
        Override ``ORCHESJOB_HOME`` on the remote host.
    poke_interval:
        Seconds between polls (passed to :class:`~airflow.sensors.base.BaseSensorOperator`).
        Defaults to ``30.0``.
    timeout:
        Maximum seconds to wait before the sensor times out.
        Defaults to ``3600.0`` (1 hour).
    """

    template_fields = ("job_id", "orchesjob_home")

    def __init__(
        self,
        *,
        job_id: str,
        ssh_conn_id: str,
        orchesjob_home: str | None = None,
        poke_interval: float = 30.0,
        timeout: float = 3600.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.job_id = job_id
        self.ssh_conn_id = ssh_conn_id
        self.orchesjob_home = orchesjob_home

    def poke(self, context: Context) -> bool:
        status_json = self._ssh_status()
        status: str = status_json.get("status", "UNKNOWN")
        self.log.info("Job %s status=%s", self.job_id, status)

        if status not in _TERMINAL_STATUSES:
            return False

        if status != "SUCCEEDED":
            raise AirflowException(
                f"Job {self.job_id} ended with status {status!r} "
                f"(exit_code={status_json.get('exit_code')})"
            )

        return True

    def _ssh_status(self) -> dict[str, Any]:
        cmd = self._build_cmd(["orchesjob", "status", "--job-id", self.job_id])
        self.log.debug("SSH command: %s", cmd)

        hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        client = hook.get_conn()
        try:
            _, stdout, stderr = client.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            out = stdout.read().decode()
            err = stderr.read().decode()
        finally:
            client.close()

        if exit_status != 0:
            raise AirflowException(
                f"orchesjob status failed (exit {exit_status}): {err.strip()}"
            )
        try:
            return json.loads(out)
        except json.JSONDecodeError as exc:
            raise AirflowException(
                f"orchesjob status returned non-JSON: {out!r}"
            ) from exc

    def _build_cmd(self, parts: list[str]) -> str:
        cmd = " ".join(shlex.quote(p) for p in parts)
        if self.orchesjob_home:
            cmd = f"ORCHESJOB_HOME={shlex.quote(self.orchesjob_home)} {cmd}"
        return cmd
