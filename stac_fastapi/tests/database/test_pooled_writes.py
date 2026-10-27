"""Pooled CRUD uses generation aliases and records only stored fields."""

import copy

import pytest
from fastapi import HTTPException
from pydantic import TypeAdapter

from stac_fastapi.extensions.transaction.request import PatchOperation
from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.database.pool_schema import (
    pool_write_alias,
    read_pool_schema,
)
from stac_fastapi.sfeos_helpers.search_engine.inserters import PooledIndexInserter


@pytest.mark.asyncio
async def test_shared_pool_aliases_route_and_isolate_duplicate_public_ids(placement):
    database, pool, collections = placement
    assert isinstance(database.async_index_inserter, PooledIndexInserter)
    for collection in collections:
        assert (
            await database.async_index_inserter.create_simple_index(
                database.client, collection
            )
            == pool
        )
        count, errors = await database.bulk_async(
            collection, [{"id": "a|b", "collection": collection}], refresh=True
        )
        assert count == 1 and errors == []
    assert (await database.client.count(index=pool))["count"] == 2
    for collection in collections:
        alias = index_alias_by_collection_id(collection)
        assert (await database.client.count(index=alias))["count"] == 1
        doc = await database.client.get(index=alias, id=mk_item_id("a|b", collection))
        assert doc["_source"]["collection"] == collection
    schema = await read_pool_schema(database.client, collections[0])
    await database.client.indices.update_aliases(
        body={
            "actions": [
                {"remove": {"index": pool, "alias": name}}
                for name in (
                    index_alias_by_collection_id(collections[0]),
                    pool_write_alias(schema),
                )
            ]
        }
    )
    count, errors = await database.bulk_async(
        collections[0], [{"id": "late", "collection": collections[0]}], refresh=True
    )
    assert count == 0 and len(errors) == 1
    assert not await database.client.indices.exists(
        index=index_alias_by_collection_id(collections[0])
    )
    assert (await database.client.count(index=pool))["count"] == 2


@pytest.mark.asyncio
async def test_pool_field_limit_failure_preserves_existing_content(
    placement, test_item
):
    database, pool, collections = placement
    item = copy.deepcopy(test_item)
    item.update(collection=collections[0])
    await database.create_item(item, refresh=True)
    before = await database.get_one_item(collections[0], item["id"])
    await database.client.indices.put_settings(
        index=pool, body={"index.mapping.total_fields.limit": 1}
    )
    item["properties"]["over_the_limit"] = True
    count, errors = await database.bulk_async(
        collections[0], [item], op_type="index", refresh=True
    )
    assert count == 0 and len(errors) == 1
    assert await database.get_one_item(collections[0], item["id"]) == before
    assert (
        "properties.over_the_limit"
        not in (await read_pool_schema(database.client, collections[0]))["fields"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_strict_partial_bulk_retains_successful_queryables(
    placement, test_item, monkeypatch, sync
):
    database, pool, collections = placement
    item = copy.deepcopy(test_item)
    item.update(id="strict-good", collection=collections[0])
    item["properties"]["strict_good_field"] = 1
    failed = copy.deepcopy(item)
    failed["id"] = "strict-bad"
    failed["properties"]["eo:cloud_cover"] = {"invalid": True}
    failed["properties"]["strict_failed_field"] = True
    settings = database.sync_settings if sync else database.async_settings
    monkeypatch.setattr(settings, "raise_on_bulk_error", True)
    with pytest.raises(Exception) as error:
        if sync:
            database.bulk_sync(
                collections[0], [item, failed], op_type="index", refresh=True
            )
        else:
            await database.bulk_async(
                collections[0], [item, failed], op_type="index", refresh=True
            )
    assert type(error.value).__name__ == "BulkIndexError"
    fields = (await read_pool_schema(database.client, collections[0]))["fields"]
    assert (
        "properties.strict_good_field" in fields
        and "properties.strict_failed_field" not in fields
    )


@pytest.mark.asyncio
async def test_routed_crud_and_stale_conditions_before_refresh(placement, test_item):
    database, pool, collections = placement
    item = copy.deepcopy(test_item)
    item.update(id="same|id", collection=collections[0])
    created = await database.create_item(item, refresh="false")
    current, version = await database.get_one_item_with_version(
        collections[0], item["id"]
    )
    assert current["collection"] == collections[0] and version.etag == created.etag
    assert await database._check_item_exists_in_collection(collections[0], item["id"])
    assert not await database._check_item_exists_in_collection(
        collections[1], item["id"]
    )
    item["properties"]["updated_by_test"] = 1
    updated = await database.create_item(
        item, upsert=True, version=version, refresh="false"
    )
    with pytest.raises(HTTPException) as stale:
        await database.create_item(item, upsert=True, version=version)
    assert stale.value.status_code == 412
    patched = await database.json_patch_item(
        collections[0],
        item["id"],
        [
            TypeAdapter(PatchOperation).validate_python(
                {"op": "add", "path": "/properties/updated_by_test", "value": 2}
            )
        ],
        "",
        version=updated,
    )
    assert patched["properties"]["updated_by_test"] == 2
    with pytest.raises(HTTPException) as stale:
        await database.delete_item(item["id"], collections[0], version=updated)
    assert stale.value.status_code == 412
    _, latest = await database.get_one_item_with_version(collections[0], item["id"])
    await database.delete_item(item["id"], collections[0], version=latest, refresh=True)
    assert (await database.client.count(index=pool))["count"] == 0
