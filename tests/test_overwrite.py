from __future__ import annotations

import io
import json

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from target_iceberg.target import TargetIceberg

CONFIG = {"db_name": "test_db", "prod": True, "overwrite_data_for_streams": '["users"]'}
SCHEMA = {"type": "object", "properties": {"id": {"type": "string"}}}


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    catalog = SqlCatalog("default", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/warehouse")
    catalog.create_namespace("test_db")
    monkeypatch.setattr("target_iceberg.sinks.load_catalog", lambda *args, **kwargs: catalog)
    return catalog


def sync(ids: list[str]) -> None:
    messages = [{"type": "SCHEMA", "stream": "users", "schema": SCHEMA, "key_properties": []}]
    messages += [{"type": "RECORD", "stream": "users", "record": {"id": i}} for i in ids]
    lines = "".join(json.dumps(m) + "\n" for m in messages)
    TargetIceberg(config=CONFIG).listen(file_input=io.StringIO(lines))


def table_ids(catalog) -> list[str]:
    return catalog.load_table("test_db.users").scan().to_arrow()["id"].to_pylist()


def test_overwrite_replaces_existing_rows(catalog):
    sync(["old"])
    sync(["new"])
    assert table_ids(catalog) == ["new"]


def test_overwrite_with_empty_stream_empties_the_table(catalog):
    sync(["old"])
    sync([])
    assert table_ids(catalog) == []
