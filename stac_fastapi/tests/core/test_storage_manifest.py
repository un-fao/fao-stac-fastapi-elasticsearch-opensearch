"""Placement configuration cannot be changed through public item identifiers."""

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError

import pytest

from stac_fastapi.sfeos_helpers.database.document import bulk_item_id_map, mk_item_id
from stac_fastapi.sfeos_helpers.database.storage import (
    collection_pool,
    load_storage_manifest,
    storage_manifest,
    storage_routing,
)


@pytest.fixture
def manifest(tmp_path, monkeypatch):
    path = tmp_path / "placement.json"
    data = {
        "version": 1,
        "pools": {
            "routed": {"index": "stac_pool_test", "routing": True},
            "unrouted": {"index": "stac_pool_other", "routing": False},
        },
        "collections": {"b|c": "routed", "c": "routed", "a,b": "unrouted"},
    }
    path.write_text(json.dumps(data))
    monkeypatch.setenv("STAC_FASTAPI_STORAGE_MANIFEST", str(path))
    monkeypatch.setenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS", "active-items")
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    yield path, data
    load_storage_manifest.cache_clear()


def test_assignment_is_immutable_and_dedicated_is_default(manifest):
    current = storage_manifest()
    with pytest.raises(TypeError):
        current.collections["other"] = "routed"
    with pytest.raises(FrozenInstanceError):
        current.pools["routed"].routing = False
    assert collection_pool("unassigned") is None
    assert mk_item_id("a|b", "unassigned") == "a|b|unassigned"
    assert storage_routing("unassigned") == {}
    assert storage_routing("a,b") == {}
    assert len(storage_routing("b|c")["routing"]) == 64


def test_pool_ids_are_unambiguous_and_bulk_resolves_public_ids(manifest):
    items = [
        {"collection": col, "id": item}
        for col, item in (
            ("b|c", "a"),
            ("c", "a|b"),
            ("b|c", "same"),
            ("c", "same"),
            ("c", "é"),
        )
    ]
    mapping = bulk_item_id_map(items)
    assert len(mapping) == len(items)
    for item in items:
        physical = mk_item_id(item["id"], item["collection"])
        assert len(physical) == 64
        assert mapping[physical] == (item["id"], item["collection"])
        assert physical == mk_item_id(item["id"], item["collection"])


@pytest.mark.parametrize(
    "invalid",
    [
        {},
        {"version": True, "pools": {}, "collections": {}},
        {"version": 2, "pools": {}, "collections": {}},
        {"version": 1, "pools": {}, "collections": {"c": "missing"}},
        {
            "version": 1,
            "pools": {"x": {"index": "items_*", "routing": True}},
            "collections": {},
        },
        {
            "version": 1,
            "pools": {"x": {"index": "stac_pool_x", "routing": "true"}},
            "collections": {},
        },
    ],
)
def test_invalid_manifest_fails_closed(manifest, invalid):
    path, _ = manifest
    path.write_text(json.dumps(invalid))
    with pytest.raises(ValueError):
        storage_manifest()


def test_pooling_requires_explicit_active_scope_and_no_datetime_partition(
    manifest, monkeypatch
):
    monkeypatch.delenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS")
    with pytest.raises(ValueError, match="active items alias"):
        storage_manifest()
    monkeypatch.setenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS", "active-items")
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "true")
    with pytest.raises(ValueError, match="datetime partitioning"):
        storage_manifest()
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")
    with pytest.raises(ValueError, match="queued generations"):
        storage_manifest()
    monkeypatch.delenv("STAC_FASTAPI_STORAGE_MANIFEST")
    assert storage_manifest() is None
    assert mk_item_id("a", "c") == "a|c"


def pool_names(index_prefix, alias_prefix=None):
    env = {**os.environ, "STAC_ITEMS_INDEX_PREFIX": index_prefix}
    env.pop("STAC_ITEMS_ALIAS_PREFIX", None)
    if alias_prefix is not None:
        env["STAC_ITEMS_ALIAS_PREFIX"] = alias_prefix
    code = """
import json
from stac_fastapi.sfeos_helpers.database.pool_schema import CONTROL_INDEX, pool_write_alias
from stac_fastapi.sfeos_helpers.database.storage import storage_alias
print(json.dumps([storage_alias('same|collection'), CONTROL_INDEX,
                  pool_write_alias({'collection': 'same|collection', 'generation': 'old'})]))
"""
    return json.loads(subprocess.check_output([sys.executable, "-c", code], env=env))


def test_pool_names_are_isolated_by_effective_item_alias_prefix():
    namespaces = [pool_names(prefix) for prefix in ("items_", "lab_a_", "lab_b_")]
    for values in zip(*namespaces):
        assert len(set(values)) == 3
        assert all(len(value) < 255 for value in values)
    assert pool_names("lab_a_") == namespaces[1]
    assert pool_names("another_index_", "lab_a_") == namespaces[1]


def test_default_pool_names_stay_compatible():
    alias = "stac_collection_" + hashlib.sha256(b"same|collection").hexdigest()
    assert pool_names("items_") == [alias, "stac_storage_control", alias + "_old"]
