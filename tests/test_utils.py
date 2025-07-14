import pytest
import pyarrow as pa
from singer_sdk.exceptions import ConfigValidationError
from target_iceberg.utils import process_json_config, to_snake_case, deduplicate_table, schemas_match

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
