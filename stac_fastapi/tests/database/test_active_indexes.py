"""Search only active storage, including the long collection-list fallback."""

import importlib
import uuid
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from stac_fastapi.sfeos_helpers.database import active_items_index, indices
from stac_fastapi.sfeos_helpers.database.index import create_index_templates_shared
from stac_fastapi.sfeos_helpers.search_engine.index_operations import IndexOperations

from ..conftest import DatabaseLogic


@pytest.mark.parametrize("alias", ["", "items_*", "a,b", "_all", "Bad", "a/b"])
def test_active_alias_must_be_explicit(monkeypatch, alias):
    monkeypatch.setenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS", alias)
    with pytest.raises(ValueError):
        active_items_index()


@pytest.mark.asyncio
async def test_missing_active_alias_blocks_startup(monkeypatch):
    monkeypatch.setenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS", "missing-active")
    client = AsyncMock()
    client.indices.exists_alias.return_value = False
    with pytest.raises(RuntimeError, match="does not exist"):
        await create_index_templates_shared(Mock(create_client=client))
    client.indices.put_index_template.assert_not_called()
    client.close.assert_awaited_once()


@pytest_asyncio.fixture
async def active_storage(monkeypatch):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("COUNT_TIMEOUT", "5")
    prefix = "items_active_test_" + uuid.uuid4().hex
    active_alias = "active-" + uuid.uuid4().hex
    monkeypatch.setenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS", active_alias)
    database = DatabaseLogic()
    names = [prefix + suffix for suffix in ("_one", "_two", "_retained")]
    try:
        for name, collection in zip(names, ("one", "two", "one")):
            aliases = {active_alias: {}} if name != names[-1] else {}
            await database.client.indices.create(
                index=name,
                body={
                    "aliases": aliases,
                    "mappings": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "collection": {"type": "keyword"},
                        }
                    },
                },
            )
            await database.client.index(
                index=name,
                id="same-id",
                body={
                    "id": name,
                    "collection": collection,
                    **({"retained_only": "value"} if name == names[-1] else {}),
                },
                refresh=True,
            )
        yield database, names, active_alias
    finally:
        for name in names:
            if await database.client.indices.exists(index=name):
                await database.client.indices.delete(index=name)
        await database.client.close()
        database.sync_client.close()


@pytest.mark.asyncio
async def test_search_and_count_exclude_open_retained_copy(active_storage):
    database, names, active_alias = active_storage
    module = importlib.import_module(DatabaseLogic.__module__)
    sort = {"id": {"order": "asc"}}
    assert indices(None) == active_alias
    items, count, _ = await database.execute_search(
        module.Search(), 10, None, sort, None, ""
    )
    assert {item["id"] for item in items} == set(names[:2])
    assert count == 2

    # WAPOR-sized selection must use the active alias and filter both requests.
    selected = ["one"] + [f"missing-collection-{n}" for n in range(667)]
    items, count, _ = await database.execute_search(
        module.Search(), 10, None, sort, selected, ""
    )
    assert [item["id"] for item in items] == [names[0]]
    assert count == 1
    items, count, _ = await database.execute_search(
        module.Search().query("term", id=names[1]),
        10,
        None,
        sort,
        selected,
        "",
    )
    assert list(items) == []
    assert count == 0
    mapping = await database.get_queryables_mapping()
    assert "collection" in mapping
    assert "retained_only" not in mapping

    result = await database.aggregate(
        collection_ids=selected,
        aggregations=["total_count"],
        search=module.Search(),
        centroid_geohash_grid_precision=1,
        centroid_geohex_grid_precision=1,
        centroid_geotile_grid_precision=1,
        geometry_geohash_grid_precision=1,
        geometry_geotile_grid_precision=1,
        datetime_frequency_interval="month",
        datetime_search="",
    )
    assert result["aggregations"]["total_count"]["value"] == 1
    assert set(await database.get_items_mapping("*")) == set(names[:2])


@pytest.mark.asyncio
async def test_new_collection_joins_active_alias(active_storage):
    database, _, active_alias = active_storage
    collection = "new-" + uuid.uuid4().hex
    name = await IndexOperations().create_simple_index(database.client, collection)
    try:
        aliases = await database.client.indices.get_alias(index=name)
        assert active_alias in aliases[name]["aliases"]
    finally:
        await database.client.indices.delete(index=name)
