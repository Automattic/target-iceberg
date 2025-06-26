"""Iceberg target class."""

from __future__ import annotations

import json
import sys
from singer_sdk import typing as th
from singer_sdk.target_base import Target
from decimal import Decimal

from target_iceberg.sinks import IcebergSink


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder for Decimal used in state."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class TargetIceberg(Target):
    """Sample target for iceberg."""

    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "prod",
            th.BooleanType,
            description="True if production db should be used, otherwise scratch will be used.",
            default=False,
        ),
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
            description='Map of column renames e.g. { "old table name": "new_table_name" }',
        ),
        th.Property(
            "table_renames",
            th.StringType,
            description='Map of table renames e.g. { "old table name": "new_table_name" }. '
                        'This is useful if you want to rename a stream if name is invalid.',
        ),
        th.Property(
            "overwrite_data_for_streams",
            th.StringType,
            description='List of stream names for which existing data should be overwritten. '
                        'e.g. [ "stream 1", "stream 2" ].'
                        'Otherwise new data will be appended.',
            default="",
        ),
        th.Property(
            "deduplicate_data_for_streams",
            th.StringType,
            description='List of stream names for which data should be deduplicated (using all columns). '
                        'e.g. [ "stream 1", "stream 2" ].'
                        'This can only be used together with upsert or data overwrite, not for incremental writing.',
            default="",
        ),
        th.Property(
            "upsert_data_for_streams",
            th.StringType,
            description="Json list of stream names for which upsert should be used. This requires a primary"
                        "key to be set either automatically via key_properties stream definition or explicitly via the"
                        "primary_key_for_streams parameter.",
            default="",
        ),
        th.Property(
            "primary_key_for_streams",
            th.StringType,
            description='Map with stream names and their list of primary keys e.g. '
                        '{ "stream1": ["column1","column2"], "stream2": "column1" }. Used only if stream is in upsert_data_for_streams.'
                        '* can be used instead of list of columns to deduplicate by all columns.',
            default="",
        ),
    ).to_dict()

    default_sink_class = IcebergSink


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger.info("Initialized target with config: %s", self.config)


    def _write_state_message(self, state: dict) -> None:
        """Emit the stream's latest state."""
        state_json = json.dumps(state, cls=DecimalEncoder)
        self.logger.info("Emitting completed target state %s", state_json)
        sys.stdout.write(f"{state_json}\n")
        sys.stdout.flush()


if __name__ == "__main__":
    TargetIceberg.cli()
