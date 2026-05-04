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

    Either ``job_id`` or ``run_key`` must be specified, but not both.

    Parameters
    ----------
    job_id:
        orchesjob job ID to monitor. Supports Jinja templating.
    run_key:
        orchesjob run key to monitor. Supports Jinja templating.
    ssh_conn_id:
        Airflow SSH Connection ID.
    orchesjob_home:
        Override ``ORCHESJOB_HOME`` on the remote host.
    poke_interval:
        Seconds between polls.
    timeout:
        Maximum seconds to wait before the sensor times out.
    """

    template_fields = ("job_id", "run_key", "orchesjob_home")

    def __init__(
        self,
        *,
        ssh_conn_id: str,
        job_id: str | None = None,
        run_key: str | None = None,
        orchesjob_home: str | None = None,
        poke_interval: float = 30.0,
        timeout: float = 3600.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)

        if (job_id is None) == (run_key is None):
            raise ValueError("Specify exactly one of job_id or run_key")

        self.job_id = job_id
        self.run_key = run_key
        self.ssh_conn_id = ssh_conn_id
        self.orchesjob_home = orchesjob_home

    def poke(self, context: Context) -> bool:
        status_json = self._ssh_status()
        status: str = status_json.get("status", "UNKNOWN")

        self.log.info(
            "Job status=%s job_id=%s run_key=%s",
            status,
            status_json.get("job_id", self.job_id),
            status_json.get("run_key", self.run_key),
        )

        if status not in _TERMINAL_STATUSES:
            return False

        if status != "SUCCEEDED":
            raise AirflowException(
                f"Job ended with status {status!r} "
                f"(job_id={status_json.get('job_id', self.job_id)}, "
                f"run_key={status_json.get('run_key', self.run_key)}, "
                f"exit_code={status_json.get('exit_code')})"
            )

        return True

    def _ssh_status(self) -> dict[str, Any]:
        parts = ["orchesjob", "status"]

        if self.job_id is not None:
            parts += ["--job-id", self.job_id]
        elif self.run_key is not None:
            parts += ["--run-key", self.run_key]
        else:
            # __init__ で弾いているため通常ここには来ない
            raise AirflowException("Neither job_id nor run_key is set")

        cmd = self._build_cmd(parts)
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
