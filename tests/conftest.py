# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Ryosuke Muraki
"""Minimal Airflow stubs so tests run without a real Airflow installation."""

import sys
from unittest.mock import MagicMock


class AirflowException(Exception):
    pass


class BaseOperator:
    def __init__(self, *, task_id: str = "test", **kwargs):
        self.task_id = task_id
        import logging
        self.log = logging.getLogger(type(self).__name__)


class BaseSensorOperator(BaseOperator):
    def __init__(self, *, poke_interval: float = 30.0, timeout: float = 3600.0, **kwargs):
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout


# Build stub modules
_airflow = MagicMock()
_airflow.exceptions.AirflowException = AirflowException
_airflow.models.BaseOperator = BaseOperator
_airflow.sensors.base.BaseSensorOperator = BaseSensorOperator
_airflow.utils.context.Context = dict
_airflow.providers.ssh.hooks.ssh.SSHHook = MagicMock

# Register stubs before any provider imports
for mod in [
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.sensors",
    "airflow.sensors.base",
    "airflow.utils",
    "airflow.utils.context",
    "airflow.providers",
    "airflow.providers.ssh",
    "airflow.providers.ssh.hooks",
    "airflow.providers.ssh.hooks.ssh",
]:
    if mod not in sys.modules:
        sys.modules[mod] = _airflow

sys.modules["airflow"].exceptions.AirflowException = AirflowException
sys.modules["airflow.exceptions"] = type(sys)("airflow.exceptions")
sys.modules["airflow.exceptions"].AirflowException = AirflowException

sys.modules["airflow.models"] = type(sys)("airflow.models")
sys.modules["airflow.models"].BaseOperator = BaseOperator

sys.modules["airflow.sensors"] = type(sys)("airflow.sensors")
sys.modules["airflow.sensors.base"] = type(sys)("airflow.sensors.base")
sys.modules["airflow.sensors.base"].BaseSensorOperator = BaseSensorOperator

sys.modules["airflow.utils"] = type(sys)("airflow.utils")
sys.modules["airflow.utils.context"] = type(sys)("airflow.utils.context")
sys.modules["airflow.utils.context"].Context = dict

_ssh_hook_mod = type(sys)("airflow.providers.ssh.hooks.ssh")
_ssh_hook_mod.SSHHook = MagicMock
sys.modules["airflow.providers"] = type(sys)("airflow.providers")
sys.modules["airflow.providers.ssh"] = type(sys)("airflow.providers.ssh")
sys.modules["airflow.providers.ssh.hooks"] = type(sys)("airflow.providers.ssh.hooks")
sys.modules["airflow.providers.ssh.hooks.ssh"] = _ssh_hook_mod
