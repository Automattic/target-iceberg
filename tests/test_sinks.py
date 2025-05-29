from __future__ import annotations

import typing as t

import pytest
from unittest import mock
from pyspark.sql.types import StringType, TimestampType
from target_iceberg.sinks import IcebergSink

TEST_CONFIG = {"db_name": "test_db", "column_renames": "old1=new1,old2=new2"}
TEST_SCHEMA = {"properties": {
        "old1": {"type": "string"},
        "old2": {"type": "string"},
        "Co l": {"type": "string"},
        "col1": {"type": "string"},
        "col2": {"type": ["null", "string"], "format": "date-time"}
    }}

def test_initialization():
    target = mock.Mock()
    target.config = TEST_CONFIG

    sink = IcebergSink(target, TEST_SCHEMA, "test", {})

    assert sink.column_renames == {'Co l': 'co_l', 'old1': 'new1', 'old2': 'new2'}


def test_type_conversion():
    target = mock.Mock()
    target.config = TEST_CONFIG

    sink = IcebergSink(target, TEST_SCHEMA, "test", {})

    assert sink.get_spark_type(TEST_SCHEMA["properties"]["col1"]) == StringType()
    assert sink.get_spark_type(TEST_SCHEMA["properties"]["col2"]) == TimestampType()


def test_to_snake_case():
    assert IcebergSink.to_snake_case("CamelCase") == "camel_case"