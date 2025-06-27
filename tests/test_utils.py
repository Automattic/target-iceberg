import pytest
import pyarrow as pa
from singer_sdk.exceptions import ConfigValidationError
from target_iceberg.utils import process_json_config, to_snake_case, deduplicate_table

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
