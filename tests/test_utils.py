import pytest
from singer_sdk.exceptions import ConfigValidationError
from target_iceberg.utils import process_json_config, clean_split, to_snake_case

def test_valid_dict_input():
    config = '{"key": "value"}'
    result = process_json_config(config, "test_config", dict)
    assert result == {"key": "value"}

def test_valid_list_input():
    config = '["item1", "item2"]'
    result = process_json_config(config, "test_list_config", list)
    assert result == ["item1", "item2"]

def test_invalid_json():
    config = '{"key": "value",a}'  # invalid due to trailing comma
    with pytest.raises(ConfigValidationError) as excinfo:
        process_json_config(config, "bad_json", dict)
    assert "Could not parse" in str(excinfo.value)

def test_wrong_type():
    config = '["item1", "item2"]'  # valid JSON but not a dict
    with pytest.raises(ConfigValidationError) as excinfo:
        process_json_config(config, "wrong_type", dict)
    assert "Invalid type for config" in str(excinfo.value)

def test_to_snake_case_basic():
    assert to_snake_case("someVariableName") == "some_variable_name"

def test_clean_and_split():
    assert clean_split(" a , , b ,c , , ", ",") == ["a", "b", "c"]
