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
            "table_name",
            th.StringType,
            title="Table name",
            description="Output table name e.g. some_db.some_table",
        ),
        th.Property(
            "max_batch_size",
            th.IntegerType,
            description="Max records to write in one batch. "
            "It can control the memory usage of the target.",
            default=10000,
        ),
        th.Property(
            "max_flatten_level",
            th.IntegerType,
            description="Max level of nesting to flatten",
            default=100,
        ),
        th.Property(
            "extra_fields",
            th.StringType,
            description="Extra fields to add to the flattened record. "
            "(e.g. extra_col1=value1,extra_col2=value2)",
        ),
        th.Property(
            "extra_fields_types",
            th.StringType,
            description="Extra fields types. (e.g. extra_col1=string,extra_col2=integer)",
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
