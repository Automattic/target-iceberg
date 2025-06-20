from __future__ import annotations

import pytest
import pyarrow as pa
from unittest import mock
from target_iceberg.sinks import IcebergSink
from target_iceberg.utils import _field_type_to_pyarrow_field

TEST_CONFIG = {"db_name": "test_db", "column_renames": '{ "old1": "new1", "old2": "new2" }'}
TEST_CONFIG_2 = {"db_name": "test_db", "table_renames": '{ "test": "test_renamed" }', "table_name_prefix": "raw"}
TEST_CONFIG_3 = {"db_name": "test_db", "overwrite_data_for_streams": '["test"]', "prod": True}
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

    target.config = TEST_CONFIG_2
    sink = IcebergSink(target, TEST_SCHEMA, "test", {})
    assert sink.table_name == "scratch.test_db__raw_test_renamed"

    target.config = TEST_CONFIG_3
    sink = IcebergSink(target, TEST_SCHEMA, "test", {})
    assert sink.overwrite_data == True


def test_to_snake_case():
    assert IcebergSink.to_snake_case("CamelCase") == "camel_case"

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
