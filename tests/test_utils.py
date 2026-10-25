from __future__ import annotations

import pyarrow as pa
import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_iceberg.utils import (
    deduplicate_table,
    flatten_schema_to_pyarrow_schema,
    process_json_config,
    schemas_match,
    to_snake_case,
)


@pytest.mark.parametrize(
    ("name", "renames", "required", "types", "nullable"),
    [
        ("org", {"org": "org_"}, ["org"], "string", False),
        ("source", {"source": "renamed"}, ["source"], "string", False),
        ("org", {"org": "org_"}, [], "string", True),
        ("unchanged", {}, ["unchanged"], "string", False),
        ("org", {"org": "org_"}, ["org"], ["null", "string"], True),
    ],
)
def test_schema_conversion_preserves_nullability(
    *,
    name: str,
    renames: dict[str, str],
    required: list[str],
    types: str | list[str],
    nullable: bool,
) -> None:
    """Preserve source nullability for automatic, configured, and unchanged names."""
    schema = {"properties": {name: {"type": types}}, "required": required}

    result = flatten_schema_to_pyarrow_schema(schema, renames)

    assert result == pa.schema(  # noqa: S101 - pytest checks the converted schema.
        [pa.field(renames.get(name, name), pa.string(), nullable=nullable)]
    )


def test_schema_conversion_uses_source_requiredness_when_names_overlap() -> None:
    """Keep requiredness attached to sources when destination names overlap them."""
    schema = {
        "properties": {
            "source": {"type": "integer"},
            "destination": {"type": "string"},
        },
        "required": ["destination"],
    }

    result = flatten_schema_to_pyarrow_schema(
        schema, {"source": "destination", "destination": "other"}
    )

    assert result == pa.schema(  # noqa: S101 - pytest checks the converted schema.
        [
            pa.field("destination", pa.int64(), nullable=True),
            pa.field("other", pa.string(), nullable=False),
        ]
    )


def test_valid_dict_input():
    config = '{"key": "value"}'
    result = process_json_config(config, "test_config", dict)
    assert result == {"key": "value"}

def test_valid_list_input():
    config = '["item1", "item2"]'
    result = process_json_config(config, "test_list_config", list)
    assert result == ["item1", "item2"]

def test_invalid_json():
    config = '{"key": "value",}'  # invalid due to trailing comma
    with pytest.raises(ConfigValidationError) as excinfo:
        process_json_config(config, "bad_json", dict)
    assert "Invalid JSON format" in str(excinfo.value)

def test_wrong_type():
    config = '["item1", "item2"]'  # valid JSON but not a dict
    with pytest.raises(ConfigValidationError) as excinfo:
        process_json_config(config, "wrong_type", dict)
    assert "Invalid type for config" in str(excinfo.value)

def test_to_snake_case_basic():
    assert to_snake_case("someVariableName") == "some_variable_name"


def test_deduplication():
    data = {
        "id": pa.array([1, 2, 2, 3, 1]),
        "value": pa.array(["a", "b", "b", "c", "a"])
    }
    table = pa.table(data)

    deduped = deduplicate_table(table)

    expected = pa.table({
        "id": pa.array([1, 2, 3]),
        "value": pa.array(["a", "b", "c"])
    })

    assert deduped == expected

def test_schemas_match_equal():
    schema1 = pa.schema([("id", pa.int64()), ("name", pa.string())])
    schema2 = pa.schema([("id", pa.int64()), ("name", pa.large_string())], metadata={b"source": b"system_a"})

    assert schemas_match(schema1, schema2)

def test_schemas_match_different_type():
    schema1 = pa.schema([("id", pa.int64())])
    schema2 = pa.schema([("id", pa.string())])

    assert not schemas_match(schema1, schema2)
