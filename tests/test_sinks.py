from __future__ import annotations

import pytest
import pyarrow as pa
from unittest import mock
from pyspark.sql.types import StringType, TimestampType
from target_iceberg.sinks import IcebergSink
from target_iceberg.utils import _field_type_to_pyarrow_field

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


def test_to_snake_case():
    assert IcebergSink.to_snake_case("CamelCase") == "camel_case"

@pytest.mark.parametrize(
    "field_name,input_types,required_fields,expected_type,expected_nullable",
    [
        ("str_column", {"type": "string"}, ["str_column"], pa.string(), False),
        ("int_column", {"type": ["null", "integer"]}, [], pa.int64(), True),
        ("int_nullable", {"anyOf": [{"type": "integer"}, {"type": "NULL"}]}, ["int_nullable"], pa.int64(), True),
        ("time_nullable", {"type": "string", "format": "date-time"}, ["time_nullable"], pa.timestamp("ms"), False),
        ("str_column", {}, ["str_column"], pa.string(), False),
    ]
)
def test_field_type_to_pyarrow_field(field_name, input_types, required_fields, expected_type, expected_nullable):
    field = _field_type_to_pyarrow_field(field_name, input_types, required_fields)
    assert field.name == field_name
    assert field.type == expected_type
    assert field.nullable == expected_nullable
