from __future__ import annotations

import typing as t

import pytest
import pyarrow as pa
from unittest import mock
from pyspark.sql.types import StringType, TimestampType
from singer_sdk.exceptions import ConfigValidationError
from target_iceberg.sinks import IcebergSink
from target_iceberg.utils import _field_type_to_pyarrow_field

TEST_SCHEMA = {"properties": {
        "old1": {"type": "string"},
        "old2": {"type": "string"},
        "Co l": {"type": "string"},
        "col1": {"type": "string"},
        "col2": {"type": ["null", "string"], "format": "date-time"}
    }}

def get_test_config(overwrte: dict = None):
    config = {"db_name": "test_db",
           "table_name_prefix": "test_table",
           "column_renames": "old1=new1,old2=new2",
           "primary_key_for_streams": "test=col1,col2;test2=col1",
           "upsert_data_for_streams": "test"}
    if overwrte:
        config.update(overwrte)
    return config

def get_test_sink(config: dict):
    target = mock.Mock()
    target.config = config
    return IcebergSink(target, TEST_SCHEMA, "test", {})

def test_initialization():
    sink = get_test_sink(get_test_config())

    assert sink.column_renames == {'Co l': 'co_l', 'old1': 'new1', 'old2': 'new2'}
    assert sink.upsert_data == True
    assert sink.overwrite_data == False
    assert sink.primary_key == ['col1', 'col2']

def test_initialization_renames_validation():
    with pytest.raises(ConfigValidationError, match="Some columns marked from rename do not exist in schema: {'invalid'}"):
        get_test_sink(get_test_config({"column_renames": "old1=new1,old2=new2,invalid=new"}))

def test_initialization_upsert_validation():
    with pytest.raises(ConfigValidationError, match="Upsert is set, but no primary key defined."):
        get_test_sink(get_test_config({"primary_key_for_streams": "invalid=invalid"}))

@pytest.mark.parametrize(
    "field_name,input_types,required_fields,expected_type,expected_nullable",
    [
        ("str_column", {"type": "string"}, ["str_column"], pa.string(), False),
        ("int_column", {"type": ["null", "integer"]}, [], pa.int64(), True),
        ("int_nullable", {"anyOf": [{"type": "integer"}, {"type": "NULL"}]}, ["int_nullable"], pa.int64(), True),
        ("time_nullable", {"type": "string", "format": "date-time"}, ["time_nullable"], pa.timestamp("us"), False),
        ("str_column", {}, ["str_column"], pa.string(), False),
    ]
)
def test_field_type_to_pyarrow_field(field_name, input_types, required_fields, expected_type, expected_nullable):
    field = _field_type_to_pyarrow_field(field_name, input_types, required_fields)
    assert field.name == field_name
    assert field.type == expected_type
    assert field.nullable == expected_nullable
