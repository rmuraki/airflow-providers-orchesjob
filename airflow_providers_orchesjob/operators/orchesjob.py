# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Ryosuke Muraki
"""Airflow operator that starts an orchesjob job over SSH."""

from __future__ import annotations

import json
import shlex
from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.context import Context


class OrchesJobOperator(BaseOperator):
    """Start a job on a remote host via ``orchesjob start`` over SSH.

    Pushes the resulting ``job_id`` to XCom (key ``job_id``) so that a
    downstream :class:`~airflow_providers_orchesjob.sensors.orchesjob.OrchesJobSensor`
    can poll for completion.

    Parameters
    ----------
    command:
        Command to execute on the remote host (passed after ``--`` to
        ``orchesjob start``).
    ssh_conn_id:
        Airflow SSH Connection ID.
    run_key:
        orchesjob idempotency key.  Defaults to
        ``{dag_id}__{task_id}__{run_id}``.
    strict:
        Pass ``--strict`` to ``orchesjob start``.  Defaults to *False*.
    orchesjob_home:
        Override ``ORCHESJOB_HOME`` on the remote host.
    """

    template_fields: Sequence[str] = ("command", "run_key", "orchesjob_home")

    def __init__(
        self,
        *,
        command: list[str],
        ssh_conn_id: str,
        run_key: str | None = None,
        strict: bool = False,
        orchesjob_home: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.ssh_conn_id = ssh_conn_id
        self.run_key = run_key
        self.strict = strict
        self.orchesjob_home = orchesjob_home

    def execute(self, context: Context) -> str:
        run_key = self._resolve_run_key(context)
        self.log.info("Starting orchesjob job with run_key=%r", run_key)

        result = self._ssh_start(run_key)
        job_id: str = result["job_id"]
        status: str = result["status"]

        self.log.info(
            "orchesjob start: accepted=%s job_id=%s status=%s",
            result.get("accepted"),
            job_id,
            status,
        )

        # Push job_id so a downstream sensor can find it
        context["ti"].xcom_push(key="job_id", value=job_id)
        return job_id

    def _resolve_run_key(self, context: Context) -> str:
        if self.run_key:
            return self.run_key
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        run_id = context["run_id"]
        return f"{dag_id}__{task_id}__{run_id}"

    def _ssh_start(self, run_key: str) -> dict[str, Any]:
        parts = ["orchesjob", "start", "--run-key", run_key]
        if self.strict:
            parts.append("--strict")
        parts += ["--"] + list(self.command)

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
                f"orchesjob start failed (exit {exit_status}): {err.strip()}"
            )
        try:
            return json.loads(out)
        except json.JSONDecodeError as exc:
            raise AirflowException(
                f"orchesjob start returned non-JSON output: {out!r}"
            ) from exc

    def _build_cmd(self, parts: list[str]) -> str:
        cmd = " ".join(shlex.quote(p) for p in parts)
        if self.orchesjob_home:
            cmd = f"ORCHESJOB_HOME={shlex.quote(self.orchesjob_home)} {cmd}"
        return cmd
