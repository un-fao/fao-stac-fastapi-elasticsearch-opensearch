"""Placement configuration cannot be changed through public item identifiers."""

import json
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
