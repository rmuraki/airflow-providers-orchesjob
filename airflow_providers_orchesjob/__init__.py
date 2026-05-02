# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Ryosuke Muraki
"""Apache Airflow provider for orchesjob."""

from __future__ import annotations


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-providers-orchesjob",
        "name": "orchesjob",
        "description": "Operator and Sensor that run jobs via orchesjob over SSH.",
        "versions": ["0.1.3"],
        "operators": [{"integration-name": "orchesjob", "python-module": "airflow_providers_orchesjob.operators.orchesjob"}],
        "sensors": [{"integration-name": "orchesjob", "python-module": "airflow_providers_orchesjob.sensors.orchesjob"}],
    }
