from __future__ import annotations

import json
from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

from airflow_providers_orchesjob.commands.reserver import OrchesJobReserveCommand


class OrchesJobReserveOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "run_key",
        "not_before",
        "expires_at",
        "metadata_json",
        "command",
    )

    def __init__(
        self,
        *,
        ssh_conn_id: str,
        run_key: str,
        command: list[str],
        not_before: str | None = None,
        expires_at: str | None = None,
        metadata: dict[str, Any] | None = None,
        metadata_json: str | None = None,
        orchesjob_reserver_bin: str = "orchesjob-reserver",
        orchesjob_start_options: list[str] | None = None,
        remote_db: str | None = None,
        remote_orchesjob_bin: str | None = None,
        command_timeout: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.run_key = run_key
        self.command = command
        self.not_before = not_before
        self.expires_at = expires_at
        self.metadata = metadata
        self.metadata_json = metadata_json
        self.orchesjob_reserver_bin = orchesjob_reserver_bin
        self.orchesjob_start_options = orchesjob_start_options or []
        self.remote_db = remote_db
        self.remote_orchesjob_bin = remote_orchesjob_bin
        self.command_timeout = command_timeout

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cmd = OrchesJobReserveCommand(
            orchesjob_reserver_bin=self.orchesjob_reserver_bin,
            run_key=self.run_key,
            command=self.command,
            not_before=self.not_before,
            expires_at=self.expires_at,
            metadata=self.metadata,
            metadata_json=self.metadata_json,
            orchesjob_start_options=tuple(self.orchesjob_start_options),
            db=self.remote_db,
            orchesjob_bin=self.remote_orchesjob_bin,
        ).shell_command()

        self.log.info("Reserve command: %s", cmd)

        hook = SSHHook(
            ssh_conn_id=self.ssh_conn_id,
            cmd_timeout=self.command_timeout,
        )
        client = hook.get_conn()

        _, stdout, stderr = client.exec_command(cmd, timeout=self.command_timeout)
        exit_status = stdout.channel.recv_exit_status()

        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")

        if err.strip():
            self.log.warning("orchesjob-reserver stderr: %s", err.strip())

        if exit_status != 0:
            raise AirflowException(
                "orchesjob-reserver reserve failed: "
                f"exit_status={exit_status}, stderr={err}, stdout={out}"
            )

        try:
            payload = json.loads(out)
        except json.JSONDecodeError as exc:
            raise AirflowException(
                f"orchesjob-reserver returned non-JSON output: {out}"
            ) from exc

        reservation = payload.get("reservation")
        if not isinstance(reservation, dict):
            raise AirflowException(f"Invalid reserve response: {payload}")

        if reservation.get("run_key") != self.run_key:
            raise AirflowException(
                f"run_key mismatch: expected={self.run_key!r}, "
                f"actual={reservation.get('run_key')!r}"
            )

        return payload
