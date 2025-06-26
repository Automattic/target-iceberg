from __future__ import annotations

import logging
from decimal import Decimal

import ast
import re
import pyarrow as pa
import json
import pyarrow.compute as pc

from singer_sdk.exceptions import ConfigValidationError

FIELD_TYPE_TO_PYARROW = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "INTEGER": pa.int64(),
    "NUMBER": pa.float64(),
    "OBJECT": pa.string(),
}

logger = logging.getLogger(__name__)


def _field_type_to_pyarrow_field(
    field_name: str, input_types: dict, required_fields: list[str]
) -> pa.Field:
    types = input_types.get("type", [])
    # If type is not defined, check if anyOf is defined
    if not types:
        for any_type in input_types.get("anyOf", []):
            if t := any_type.get("type"):
                if isinstance(t, list):
                    types.extend(t)
                else:
                    types.append(t)
    types = [types] if isinstance(types, str) else types
    types_uppercase = [item.upper() for item in types]
    nullable = "NULL" in types_uppercase or field_name not in required_fields
    if "NULL" in types_uppercase:
        types_uppercase.remove("NULL")
    input_type = next(iter(types_uppercase)) if types_uppercase else ""
    pyarrow_type = FIELD_TYPE_TO_PYARROW.get(input_type, pa.string())
    # override with timestamp type if format set to date or date-time even if value type is e.g. string.
    if input_types.get("format") in ["date", "date-time"]:
        pyarrow_type = pa.timestamp("us", tz="UTC")
    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(flatten_schema_dictionary: dict, column_renames: dict) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order.

    E.g:
     dictionary = {
        'properties': {
             'key_1': {'type': ['null', 'integer']},
             'key_2__key_3': {'type': ['null', 'string']},
             'key_2__key_4__key_5': {'type': ['null', 'integer']},
             'key_2__key_4__key_6': {'type': ['null', 'array']}
           }
        }
    By calling the function with the dictionary above as parameter,
    you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    flatten_schema = flatten_schema_dictionary.get("properties", {})
    required_fields = flatten_schema_dictionary.get("required", [])
    return pa.schema(
        [
            _field_type_to_pyarrow_field(
                column_renames.get(field_name, field_name), field_input_types, required_fields=required_fields
            )
            for field_name, field_input_types in flatten_schema.items()
        ]
    )


def _convert_decimal(value):
    """Convert Decimal"""
    if isinstance(value, Decimal):
        return float(value)
    return value


def create_pyarrow_table(list_dict: list[dict], schema: pa.Schema) -> pa.Table:
    """Create a pyarrow Table from a python list of dict."""
    data = {f: [_convert_decimal(row.get(f)) for row in list_dict] for f in schema.names}
    return pa.table(data).cast(schema)

def to_snake_case(text: str) -> str:
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', text).lower()

def process_json_config(config, config_name, expected_type):
    try:
        value = json.loads(config)
    except json.JSONDecodeError as e:
        raise ConfigValidationError(f"Invalid JSON format for config {config_name}: {config}. \nError: {e}")

    if not isinstance(value, expected_type):
        raise ConfigValidationError(
            f"Invalid type for config {config_name}: {type(value)} -> {config}. \nExpected type: {expected_type}."
        )
    return value

def deduplicate_table(table: pa.Table) -> pa.Table:
    struct_array = pa.StructArray.from_arrays(
        [table[col] for col in table.column_names],
        fields=[table.schema.field(col) for col in table.column_names]
    )
    unique_indices = pc.unique_indices(struct_array)
    return table.take(unique_indices)
