"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.helpers._flattening import flatten_schema, flatten_record
from singer_sdk.sinks import BatchSink

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
import os

class IcebergSink(BatchSink):
    def __init__(self, target, schema, stream_name, key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        # Accessing the properties from the target's config
        self.table_name = self.config.get("table_name")
        self.flatten_max_level = self.config.get("max_flatten_level", 100)
        #self.hive_thrift_uri = self.config.get("hive_thrift_uri")
        #self.warehouse_uri = self.config.get("warehouse_uri")
        #self.partition_by = self.config.get("partition_by", [])

        # Extra fields
        self.extra_values = (
            dict([kv.split("=") for kv in self.config["extra_fields"].split(",")])
            if self.config.get("extra_fields")
            else {}
        )
        self.extra_values_types = {}
        if self.config.get("extra_fields_types"):
            for field_type in self.config["extra_fields_types"].split(","):
                field_name, _type = field_type.split("=")
                self.extra_values_types[field_name] = {"type": [_type]}

        self.flatten_schema = flatten_schema(
            self.schema, max_level=self.flatten_max_level
        )
        self.flatten_schema.get("properties", {}).update(self.extra_values_types)

        self.validation()

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("max_batch_size", 10000)

    def validation(self) -> None:
        """Extra fields and Partition Cols validation."""
        assert bool(self.extra_values) == bool(
            self.extra_values_types
        ), "extra_fields and extra_fields_types must be both set or both unset"
        if self.extra_values:
            assert (
                self.extra_values.keys() == self.extra_values_types.keys()
            ), "extra_fields and extra_fields_types must have the same keys"

    #def start_batch(self, context: dict) -> None:
    #    self.rows = []

    def process_record(self, record: dict, context: dict) -> None:
        record_flatten = (
            flatten_record(
                record,
                flattened_schema=self.flatten_schema,
                max_level=self.flatten_max_level,
            )
            | self.extra_values
        )
        super().process_record(record_flatten, context)
        #self.rows.append(record)

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
        partition_size = (os.cpu_count())*3
        conf = SparkConf() \
            .setAppName("Apache Iceberg with PySpark") \
            .setMaster("local[*]")
        """\
            .setAll([
                ("spark.driver.memory", "4g"),
                ("spark.executor.memory", "4g"),
                ("spark.sql.shuffle.partitions", f"{partition_size}"),
                ('spark.sql.adaptive.coalescePartitions.initialPartitionNum', f"{(os.cpu_count())}"),
                ('spark.sql.adaptive.coalescePartitions.parallelismFirst', 'false'),
                ('spark.sql.files.minPartitionNum', "1"),
                ('spark.sql.files.maxPartitionBytes', '500mb'),

                # Add Iceberg SQL extensions like UPDATE or DELETE in Spark
                ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

                # Register `hive_catalog`
                ("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog"),
                ('spark.sql.catalog.hive_catalog.type', 'hive'),
                ('spark.sql.catalog.hive_catalog.uri', self.hive_thrift_uri),
                ('spark.sql.catalog.hive_catalog.warehouse', self.warehouse_uri),
            ])"""
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

        return spark

    def create_dataframe(self, spark: SparkSession, records: list):
        rows_rdd = spark.sparkContext.parallelize(records)
        rows = rows_rdd.map(lambda x: Row(**x))
        # Function to clean field names
        #def clean_field_name(name):
            #return re.sub(r'[\s\.,]+', '_', name)
        df = spark.createDataFrame(rows)
        #for col_name in df.columns:
            #df = df.withColumnRenamed(col_name, clean_field_name(col_name))
        return df

    def create_table(self, spark: SparkSession, df: DataFrame):
        #table_name = f"hive_catalog.default.{self.table_name}"

        # Check if the table exists
        if spark.catalog.tableExists(self.table_name):
            spark.sql(f"REFRESH TABLE {self.table_name}")

        # Retrieve the schema of the DataFrame
        schema = df.schema

        # Build a string of column definitions
        column_definitions = ', '.join([f"{field.name} {field.dataType.simpleString()}" for field in schema.fields])

        # Construct the partition clause based on self.partition_by
        partition_clause = ""
        #if self.partition_by:
        #    partition_keys = ', '.join(self.partition_by)
        #    partition_clause = f"PARTITIONED BY ({partition_keys})"

        # SQL query to create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {column_definitions}
        ) USING iceberg {partition_clause};
        """

        # Execute the query
        spark.sql(create_table_query)

    def write_data(self, spark: SparkSession, df: DataFrame):
        #table_name = f"hive_catalog.default.{self.table_name}"
        df \
        .writeTo(f"{self.table_name}") \
        .append()
