from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OrchesJobReserveCommand:
    run_key: str
    command: list[str]
    not_before: str | None = None
    expires_at: str | None = None
    metadata_json: str | None = None
    metadata: dict[str, Any] | None = None
    orchesjob_reserver_bin: str = "orchesjob-reserver"
    orchesjob_start_options: tuple[str, ...] = ()
    db: str | None = None
    orchesjob_bin: str | None = None
    env: dict[str, str] | None = None

    def argv(self) -> list[str]:
        args: list[str] = [
            self.orchesjob_reserver_bin,
            "reserve",
            "--run-key",
            self.run_key,
        ]

        if self.db:
            args += ["--db", self.db]

        if self.orchesjob_bin:
            args += ["--orchesjob-bin", self.orchesjob_bin]

        if self.not_before:
            args += ["--not-before", self.not_before]

        if self.expires_at:
            args += ["--expires-at", self.expires_at]

        metadata_json = self.metadata_json
        if metadata_json is None and self.metadata is not None:
            metadata_json = json.dumps(
                self.metadata,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        if metadata_json:
            args += ["--metadata-json", metadata_json]

        for opt in self.orchesjob_start_options:
            args += ["--orchesjob-start-option", opt]

        args += ["--", *self.command]
        return args

    def shell_command(self) -> str:
        env_prefix = ""
        if self.env:
            env_prefix = " ".join(
                f"{shlex.quote(str(k))}={shlex.quote(str(v))}"
                for k, v in self.env.items()
            )
            env_prefix += " "

        return env_prefix + " ".join(shlex.quote(str(x)) for x in self.argv())
