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
            "db_name",
            th.StringType,
            title="Db name",
            description="Output Db name e.g. some_db. Table name is derived from stream name automatically.",
        ),
        th.Property(
            "table_name_prefix",
            th.StringType,
            title="Table name prefix",
            description="Additional table name prefix e.g. 'a4a' would result in a table name like 'my_db.a4a_raw_users'.",
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
            default=0,
        ),
        th.Property(
            "skip_add_synced_field",
            th.BooleanType,
            description="Skip adding synced_ms column",
        ),
        th.Property(
            "column_renames",
            th.StringType,
            description="List of column renames e.g. 'oldname1=newname1,oldname2=newname2'",
        ),
    ).to_dict()

    default_sink_class = IcebergSink


    def process_endofpipe(self) -> None:
        def find_decimals(obj, path=""):
            decimals = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_path = f"{path}.{k}" if path else k
                    decimals.extend(find_decimals(v, new_path))
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_path = f"{path}[{i}]"
                    decimals.extend(find_decimals(item, new_path))
            elif isinstance(obj, Decimal):
                decimals.append((path, obj))
            return decimals

        decimals_found = find_decimals(self._latest_state)

        if decimals_found:
            msg = "State contains unserializable Decimal values:\n"
            msg += "\n".join([f"{path} = {value}" for path, value in decimals_found])
            raise TypeError(msg)
        else:
            raise Exception(f"Nie znaleziono decimal: {self._latest_state}")


if __name__ == "__main__":
    TargetIceberg.cli()
