"""Iceberg target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_iceberg.sinks import (
    IcebergSink,
)


class TargetIceberg(Target):
    """Sample target for iceberg."""

    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "filepath",
            th.StringType,
            title="Output File Path",
            description="The path to the target output file",
        ),
        th.Property(
            "file_naming_scheme",
            th.StringType,
            title="File Naming Scheme",
            description="The scheme with which output files will be named",
        ),
        th.Property(
            "auth_token",
            th.StringType,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The path to the target output file",
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
