"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations

import re
from datetime import datetime
from functools import cached_property

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from singer_sdk.helpers._flattening import flatten_record, flatten_schema
from singer_sdk.sinks import BatchSink
from singer_sdk.exceptions import ConfigValidationError

from target_iceberg.utils import (create_pyarrow_table, flatten_schema_to_pyarrow_schema,
                                  process_config_replace, clean_split, to_snake_case)


SYNCED_COLUMN_NAME = "synced_ms"

class IcebergSink(BatchSink):
    def __init__(self, target, schema, stream_name, key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        self.flatten_max_level = self.config.get("max_flatten_level", 0)
        self.skip_add_synced_field = self.config.get("skip_add_synced_field", False)
        self.data_buffer = None
        self.start_time = datetime.utcnow()

        self.validate_config()

    def validate_config(self) -> None:
        # Check column renames
        missing_keys = set(self.column_renames.keys()) - set(self.flatten_schema.get("properties", {}).keys())
        if missing_keys:
            raise ConfigValidationError(f"Some columns marked from rename do not exist in schema: {missing_keys}")

        # Check primary key if upsert is set
        if self.upsert_data and not self.primary_key:
            raise ConfigValidationError("Upsert is set, but no primary key defined.")

    @cached_property
    def pyarrow_schema(self):
        return flatten_schema_to_pyarrow_schema(self.flatten_schema, self.column_renames)

    @cached_property
    def table_name(self) -> str:
        snake_case_stream_name = to_snake_case(self.stream_name)
        table_renames = process_config_replace(self.config.get("table_renames"))
        if self.stream_name in table_renames or '*' in table_renames:
            snake_case_stream_name = table_renames.get(self.stream_name, table_renames.get('*', snake_case_stream_name))
        table_name_prefix = f"{self.config.get('table_name_prefix')}_" if self.config.get("table_name_prefix") else ""
        if self.config.get('prod'):
            return f"{self.config['db_name']}.{table_name_prefix}{snake_case_stream_name}"
        else:
            return f"scratch.{self.config['db_name']}__{table_name_prefix}{snake_case_stream_name}"

    @cached_property
    def flatten_schema(self):
        result = flatten_schema(self.schema, max_level=self.flatten_max_level)
        if not self.skip_add_synced_field:
            result.get("properties", {}).update({SYNCED_COLUMN_NAME: {"type": "STRING", "format": "date-time"}})

        return result

    @cached_property
    def column_renames(self) -> dict[str, str]:
        result = {key: re.sub(r'[\s\.,]+', '_', key).lower()
                               for key in self.flatten_schema.get("properties", {}).keys()}
        result.update(process_config_replace(self.config.get("column_renames")))
        return {key: value for key, value in result.items() if key != value}

    @cached_property
    def overwrite_data(self) -> bool:
        return bool([s for s in self.config.get("overwrite_data_for_streams", '').split(',')
                                    if s.strip().lower() == self.stream_name.lower()])

    @cached_property
    def upsert_data(self) -> bool:
        return bool([s for s in self.config.get("upsert_data_for_streams", '').split(',')
                                    if s.strip().lower() == self.stream_name.lower()])

    @cached_property
    def primary_key(self) -> list[str]:
        primary_key_for_streams = self.config.get("primary_key_for_streams", '').lower()
        streams = [clean_split(s, '=') for s in clean_split(primary_key_for_streams, ';')]

        if not all(len(s) == 2 and s[0] and s[1] for s in streams):
            raise ConfigValidationError(f"Invalid format of primary_key_for_streams: {primary_key_for_streams}")
        streams = {stream[0]: clean_split(stream[1], ',') for stream in streams}
        key = streams.get(self.stream_name.lower(), self.key_properties)

        if key == ["*"]:
            key = [k for k in self.pyarrow_schema.names if k != SYNCED_COLUMN_NAME]

        assert set(key).issubset(set(self.pyarrow_schema.names)), \
            f"Some columns of the primary key {key} do not exist in table schema: {self.pyarrow_schema.names}"

        return key

    @cached_property
    def catalog(self):
        return load_catalog("default")

    @cached_property
    def table(self):
        return self.get_table()

    @cached_property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("max_batch_size", 10000)

    def process_record(self, record: dict, context: dict) -> None:
        record_flatten = (
            flatten_record(
                record,
                flattened_schema=self.flatten_schema,
                max_level=self.flatten_max_level,
            )
        )
        for old_name, new_name in self.column_renames.items():
            record_flatten[new_name] = record_flatten.pop(old_name, None)
        if not self.skip_add_synced_field:
            record_flatten = record_flatten | { SYNCED_COLUMN_NAME: self.start_time }
        super().process_record(record_flatten, context)

    def process_batch(self, context: dict) -> None:
        self.logger.info(f'Processing batch for {self.stream_name} - table {self.table_name} '
                         f'with {len(context["records"])} records.')
        new_data = create_pyarrow_table(context.get("records", []), self.pyarrow_schema)
        self.logger.info(f"Pyarrow table size: {new_data.nbytes} | ({len(new_data)} rows)")
        if self.overwrite_data:
            # If data is to be overwritten, we buffer it all in memory and write at the end
            self.data_buffer = pa.concat_tables([self.data_buffer, new_data]) if self.data_buffer else new_data
        else:
            if self.upsert_data:
                self.table.upsert(new_data, join_cols=self.primary_key)
            else:
                self.table.append(new_data)

        del context["records"]

    def get_table(self):
        if not self.catalog.table_exists(self.table_name):
            self.logger.info(f'Table {self.table_name} does not exist, so creating it')
            return self.catalog.create_table(self.table_name, schema=self.pyarrow_schema)
        else:
            return self.catalog.load_table(self.table_name)

    def clean_up(self) -> None:
        """Perform any clean up actions required at end of a stream."""
        if self.overwrite_data:
            self.logger.info(f'Overwriting data in the table {self.table_name}')
            self.table.overwrite(self.data_buffer)
        super().clean_up()
