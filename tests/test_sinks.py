from __future__ import annotations

import pytest
import pyarrow as pa
from unittest import mock
from singer_sdk.exceptions import ConfigValidationError
from target_iceberg.sinks import IcebergSink
from target_iceberg.utils import _field_type_to_pyarrow_field

TEST_CONFIG = {"db_name": "test_db", "column_renames": '{ "old1": "new1", "old2": "new2" }',
               "upsert_data_for_streams": '["test"]', "deduplicate_data_for_streams": '["test"]',
               "primary_key_for_streams": '{ "test": ["new1"] }'}
TEST_CONFIG_2 = {"db_name": "test_db", "table_renames": '{ "test": "test_renamed" }', "table_name_prefix": "raw",
                 "table_name_suffix": "suf"}
TEST_CONFIG_3 = {"db_name": "test_db", "overwrite_data_for_streams": '["test"]', "prod": True}

TEST_SCHEMA = {"properties": {
        "public": {"type": "string"},
        "old1": {"type": "string"},
        "old2": {"type": "string"},
        "Co l": {"type": "string"},
        "col1": {"type": "string"},
        "col2": {"type": ["null", "string"], "format": "date-time"}
    }}

def get_test_sink(config_base: dict, config_overwrites: dict = None):
    target = mock.Mock()
    config = config_base.copy()
    if config_overwrites:
        config.update(config_overwrites)
    target.config = config
    return IcebergSink(target, TEST_SCHEMA, "test", {})

def test_initialization():
    sink = get_test_sink(TEST_CONFIG)
    assert sink.column_renames == {'public': 'public_', 'Co l': 'co_l', 'old1': 'new1', 'old2': 'new2'}
    assert sink.table_name == "scratch.test_db__test"
    assert sink.upsert_data == True
    assert sink.deduplicate_data == True
    assert sink.primary_key == ["new1"]

    sink = get_test_sink(TEST_CONFIG_2)
    assert sink.table_name == "scratch.test_db__raw_test_renamed_suf"

    sink = get_test_sink(TEST_CONFIG_3)
    assert sink.overwrite_data == True
    assert sink.table_name == "test_db.test"

def test_initialization_renames_validation():
    with pytest.raises(ConfigValidationError, match="Some columns marked from rename do not exist in schema: {'invalid'}"):
        get_test_sink(TEST_CONFIG, {"column_renames": '{ "old1": "new1", "old2": "new2", "invalid": "new" }'})

def test_initialization_upsert_validation():
    with pytest.raises(ConfigValidationError, match="Upsert is set, but no primary key defined."):
        get_test_sink(TEST_CONFIG, {"primary_key_for_streams": '{"invalid": ["invalid"]}'})

@pytest.mark.parametrize(
    "field_name,input_types,required_fields,expected_type,expected_nullable",
    [
        ("str_column", {"type": "string"}, ["str_column"], pa.string(), False),
        ("int_column", {"type": ["null", "integer"]}, [], pa.int64(), True),
        ("int_nullable", {"anyOf": [{"type": "integer"}, {"type": "NULL"}]}, ["int_nullable"], pa.int64(), True),
        ("time_nullable", {"type": "string", "format": "date-time"}, ["time_nullable"], pa.timestamp("us", tz="UTC"), False),
        ("str_column", {}, ["str_column"], pa.string(), False),
    ]
)
def test_field_type_to_pyarrow_field(field_name, input_types, required_fields, expected_type, expected_nullable):
    field = _field_type_to_pyarrow_field(field_name, input_types, required_fields)
    assert field.name == field_name
    assert field.type == expected_type
    assert field.nullable == expected_nullable


@pytest.mark.parametrize(
    ("mode", "stored_nullable", "writer_nullable", "mixed"),
    [
        ("overwrite", True, False, False),
        ("overwrite", False, True, False),
        ("append", False, True, False),
        ("upsert", False, True, False),
        ("overwrite", True, False, True),
    ],
)
def test_table_rejects_unsafe_nullability_before_mutation(
    *,
    mode: str,
    stored_nullable: bool,
    writer_nullable: bool,
    mixed: bool,
) -> None:
    """Reject unsafe nullability even when another schema difference exists."""
    config = {"db_name": "test_db", "skip_add_synced_field": True}
    if mode != "append":
        config[f"{mode}_data_for_streams"] = '["organizations"]'
    target = mock.Mock()
    target.config = config
    schema = {
        "properties": {
            "id": {"type": "integer"},
            "org": {"type": ["null", "string"] if writer_nullable else "string"},
        },
        "required": ["id", "org"],
    }
    sink = IcebergSink(target, schema, "organizations", ["id"])
    catalog = mock.Mock()
    existing_table = catalog.load_table.return_value
    existing_table.schema.return_value.as_arrow.return_value = pa.schema(
        [
            pa.field("id", pa.string() if mixed else pa.int64(), nullable=False),
            pa.field("org_", pa.large_string(), nullable=stored_nullable),
        ]
    )
    sink.catalog = catalog

    with pytest.raises(
        ConfigValidationError, match="incompatible nullability for columns org_"
    ):
        _ = sink.table

    catalog.drop_table.assert_not_called()
    catalog.create_table.assert_not_called()
    existing_table.append.assert_not_called()
    existing_table.upsert.assert_not_called()
    existing_table.overwrite.assert_not_called()


@pytest.mark.parametrize(
    "mode", ["append", "upsert", "overwrite", "new", "type-change"]
)
def test_table_preserves_supported_schema_paths(mode: str) -> None:
    """Keep compatible writes, new tables, and unrelated schema changes working."""
    config = {"db_name": "test_db", "skip_add_synced_field": True}
    if mode in {"overwrite", "type-change"}:
        config["overwrite_data_for_streams"] = '["organizations"]'
    elif mode == "upsert":
        config["upsert_data_for_streams"] = '["organizations"]'
    target = mock.Mock()
    target.config = config
    schema = {
        "properties": {"id": {"type": "integer"}, "org": {"type": "string"}},
        "required": ["id", "org"],
    }
    sink = IcebergSink(target, schema, "organizations", ["id"])
    catalog = mock.Mock()
    catalog.table_exists.return_value = mode != "new"
    existing_table = catalog.load_table.return_value
    existing_table.schema.return_value.as_arrow.return_value = pa.schema(
        [
            pa.field(
                "id", pa.string() if mode == "type-change" else pa.int64(),
                nullable=False,
            ),
            pa.field("org_", pa.large_string(), nullable=mode in {"append", "upsert"}),
        ]
    )
    sink.catalog = catalog

    result = sink.table

    if mode in {"new", "type-change"}:
        assert result is catalog.create_table.return_value  # noqa: S101 - pytest result.
        catalog.create_table.assert_called_once_with(
            sink.table_name, schema=sink.pyarrow_schema
        )
    else:
        assert result is existing_table  # noqa: S101 - pytest result.
        catalog.create_table.assert_not_called()
    if mode == "type-change":
        catalog.drop_table.assert_called_once_with(sink.table_name)
    else:
        catalog.drop_table.assert_not_called()
