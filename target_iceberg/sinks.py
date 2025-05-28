"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from singer_sdk.helpers._flattening import flatten_schema, flatten_record
from singer_sdk.sinks import BatchSink

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType
import re
import os

class IcebergSink(BatchSink):
    def __init__(self, target, schema, stream_name, key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        table_name_prefix = f"{self.config.get('table_name_prefix')}_" if self.config.get("table_name_prefix") else ""
        self.table_name = f"{self.config['db_name'] if self.config.get('prod') else 'scratch'}.{table_name_prefix}{self.__class__.to_snake_case(self.stream_name)}"
        self.flatten_max_level = self.config.get("max_flatten_level", 0)
        self.skip_add_synced_field = self.config.get("skip_add_synced_field", False)

        self.flatten_schema = flatten_schema(
            self.schema, max_level=self.flatten_max_level
        )
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

        self.spark_schema = StructType([
            StructField(self.column_renames.get(name, name), self.get_spark_type(dtype), True)
            for name, dtype in self.flatten_schema["properties"].items()
        ])
        self.spark_schema_field_set = set(self.spark_schema.fieldNames())

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

    def _write_state_message(self, state: dict) -> None:
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

        decimals_found = find_decimals(state)

        if decimals_found:
            msg = "State contains unserializable Decimal values:\n"
            msg += "\n".join([f"{path} = {value}" for path, value in decimals_found])
            raise TypeError(msg)

        super()._write_state_message(state)


    def process_record(self, record: dict, context: dict) -> None:
        record_flatten = flatten_record(
            record,
            flattened_schema=self.flatten_schema,
            max_level=self.flatten_max_level,
        )
        # rename columns
        for old_name, new_name in self.column_renames.items():
            record_flatten[new_name] = record_flatten.pop(old_name)
        # filter out fields which aren't properly defined in schema
        record_flatten = {k: v for k, v in record_flatten.items() if k in self.spark_schema_field_set}
        record_flatten = {
            # Convert decimal and int values to double to avoid type mismatch exceptions
            k: float(v) if isinstance(v, Decimal) or (
                    isinstance(v, int) and isinstance(self.spark_schema[k].dataType, DoubleType))
            else v
            for k, v in record_flatten.items()
        }
        if not self.skip_add_synced_field:
            record_flatten = record_flatten | { "synced_ms": self.start_time }
        super().process_record(record_flatten, context)

    def get_spark_type(self, col):
        col_type = col["type"]
        if col.get("format") in ["date", "date-time"]:
            return TimestampType()
        else:
            # type can be a value or a list e.g. ["null", "string"]
            col_type_list = col_type if isinstance(col_type, list) else [col_type]
            col_type_list = [col_type for col_type in col_type_list if col_type.lower() != "null"]
            col_type = col_type_list[0].lower()
            return {
                "string": StringType(),
                "integer": LongType(),
                "number": DoubleType(),
                "boolean": BooleanType(),
                "timestamp": TimestampType(),
                "object": StringType(),
                "array": StringType(),
            }[col_type]

    def process_batch(self, context: dict) -> None:
        self.logger.info(
            f'Processing batch for {self.stream_name} with {len(context["records"])} records.'
        )
        spark = self.init_spark()
        df = spark.createDataFrame(context.get("records", []), schema=self.spark_schema)
        self.create_table(spark, df)
        self.write_data(spark, df)

        del context["records"]

    def init_spark(self):
        conf = SparkConf() \
            .setAppName("Apache Iceberg with PySpark") \
            .setMaster("local[*]")

        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

        return spark

    def create_dataframe(self, spark: SparkSession, records: list, schema: StructType) -> DataFrame:
        spark.createDataFrame(records, schema=schema)

    def create_table(self, spark: SparkSession, df: DataFrame):
        if not spark.catalog.tableExists(self.table_name):
            column_definitions = ', '.join(
                [f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields])

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {column_definitions}
            ) USING iceberg;
            """

            self.logger.info(
                f'Table {self.table_name} does not exist, so running create table statement:\n{create_table_query}'
            )
            spark.sql(create_table_query)

    def write_data(self, spark: SparkSession, df: DataFrame):
        df \
        .writeTo(f"{self.table_name}") \
        .append()
