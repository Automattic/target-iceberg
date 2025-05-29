"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations

import os
import re
from datetime import datetime
from functools import cached_property

from pyiceberg.catalog import load_catalog
from singer_sdk.helpers._flattening import flatten_record, flatten_schema
from singer_sdk.sinks import BatchSink

from target_iceberg.utils import create_pyarrow_table, flatten_schema_to_pyarrow_schema

class IcebergSink(BatchSink):
    spark = None
    def __init__(self, target, schema, stream_name, key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        table_name_prefix = f"{self.config.get('table_name_prefix')}_" if self.config.get("table_name_prefix") else ""
        self.table_name = (f"{self.config['db_name'] if self.config.get('prod') else 'scratch'}."
                           f"{table_name_prefix}{self.__class__.to_snake_case(self.stream_name)}")
        self.flatten_max_level = self.config.get("max_flatten_level", 0)
        self.skip_add_synced_field = self.config.get("skip_add_synced_field", False)

        self.flatten_schema = flatten_schema(self.schema, max_level=self.flatten_max_level)
        if not self.skip_add_synced_field:
            self.flatten_schema.get("properties", {}).update({"synced_ms": {"type": "timestamp"}})
        self.start_time = datetime.utcnow()

        self.column_renames = {key: re.sub(r'[\s\.,]+', '_', key).lower()
                               for key in self.flatten_schema.get("properties", {}).keys()}
        self.column_renames.update(
            dict([kv.split("=") for kv in self.config["column_renames"].split(",")])
            if self.config.get("column_renames")
            else {}
        )
        self.column_renames = {key: value for key, value in self.column_renames.items() if key != value}

        missing_keys = set(self.column_renames.keys()) - set(self.flatten_schema.get("properties", {}).keys())
        assert not missing_keys, f"Some columns marked from rename do not exist in schema: {missing_keys}"

        self.pyarrow_schema = flatten_schema_to_pyarrow_schema(self.flatten_schema, self.column_renames)

    @cached_property
    def catalog(self):
        return load_catalog("default")

    @cached_property
    def table(self):
        return self.get_table()

    @staticmethod
    def to_snake_case(text: str):
        return re.sub(r'([a-z])([A-Z])', r'\1_\2', text).lower()

    @property
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
            record_flatten[new_name] = record_flatten.pop(old_name)
        if not self.skip_add_synced_field:
            record_flatten = record_flatten | { "synced_ms": self.start_time }
        super().process_record(record_flatten, context)

    def process_batch(self, context: dict) -> None:
        self.logger.info(
            f'Processing batch for {self.stream_name} with {len(context["records"])} records.'
        )
        pyarrow_df = create_pyarrow_table(context.get("records", []), self.pyarrow_schema)
        self.logger.info(
            f"Pyarrow table size: {pyarrow_df.nbytes} | ({len(pyarrow_df)} rows)"
        )
        self.table.append(pyarrow_df)
        del context["records"]

    def get_table(self):
        if not self.catalog.table_exists(self.table_name):
            self.logger.info(f'Table {self.table_name} does not exist, so creating it')
            return self.catalog.create_table(self.table_name, schema=self.pyarrow_schema)
        else:
            return self.catalog.load_table(self.table_name)
