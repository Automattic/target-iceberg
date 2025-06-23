import pytest
from singer_sdk.exceptions import ConfigValidationError
from target_iceberg.utils import process_config_replace, clean_split, to_snake_case

@pytest.mark.parametrize(
    "config_str, expected",
    [
        ("foo=bar", {"foo": "bar"}),
        ("a=1,b=2", {"a": "1", "b": "2"}),
        ("", {}),       # Empty string
        (None, {}),     # None input
    ]
)
def test_process_config_replace_valid(config_str, expected):
    assert process_config_replace(config_str) == expected

def test_process_config_replace_invalid_format():
    with pytest.raises(ConfigValidationError, match="Invalid format for a=1,badformat: badformat. Expected format is 'key=value'."):
        process_config_replace("a=1,badformat")
    with pytest.raises(ConfigValidationError, match="Invalid format for a==1: a==1. Expected format is 'key=value'."):
        process_config_replace("a==1")

def test_to_snake_case_basic():
    assert to_snake_case("someVariableName") == "some_variable_name"

def test_clean_and_split():
    assert clean_split(" a , , b ,c , , ", ",") == ["a", "b", "c"]