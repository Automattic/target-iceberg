"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
from datetime import datetime

from singer_sdk.helpers._flattening import flatten_schema, flatten_record
from singer_sdk.sinks import BatchSink

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
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
        self.table_name = self.config.get("table_name")
        self.flatten_max_level = self.config.get("max_flatten_level", 0)
        self.skip_add_synced_field = self.config.get("skip_add_synced_field", False)
        self.column_renames = (
            dict([kv.split("=") for kv in self.config["column_renames"].split(",")])
            if self.config.get("column_renames")
            else {}
        )

        self.flatten_schema = flatten_schema(
            self.schema, max_level=self.flatten_max_level
        )
        self.flatten_schema.get("properties", {})
        self.start_time_ms = int(datetime.utcnow().timestamp() * 1000)

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
            | { "synced_ms": self.start_time_ms } if not self.skip_add_synced_field else {}
        )
        super().process_record(record_flatten, context)

    def process_batch(self, context: dict) -> None:
        self.logger.info(
            f'Processing batch for {self.stream_name} with {len(context["records"])} records.'
        )

        spark = self.init_spark()
        df = self.create_dataframe(spark, context.get("records", []))
        self.create_table(spark, df)
        self.write_data(spark, df)

        del context["records"]

    def init_spark(self):
        conf = SparkConf() \
            .setAppName("Apache Iceberg with PySpark") \
            .setMaster("local[*]")

        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

        return spark

    def create_dataframe(self, spark: SparkSession, records: list):
        rows_rdd = spark.sparkContext.parallelize(records)
        rows = rows_rdd.map(lambda x: Row(**x))
        def clean_field_name(name):
            return re.sub(r'[\s\.,]+', '_', name).lower()
        df = spark.createDataFrame(rows)
        for old, new in self.column_renames.items():
            df = df.withColumnRenamed(old, new)
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, clean_field_name(col_name))
        return df

    def create_table(self, spark: SparkSession, df: DataFrame):
        # Check if the table exists
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
