"""Deleting one pooled collection preserves its neighbors and rejects stale writers."""

import asyncio
import copy
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import TypeAdapter

from stac_fastapi.extensions.transaction.request import PatchOperation
from stac_fastapi.sfeos_helpers.database import mk_item_id
from stac_fastapi.sfeos_helpers.database.pool_schema import (
    CONTROL_INDEX,
    read_pool_schema,
    record_pool_fields,
)
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX
from stac_fastapi.types.errors import ConflictError, NotFoundError


@pytest.mark.asyncio
@pytest.mark.parametrize("reserved_type", ["CreatingCollection", "DeletingCollection"])
async def test_storage_reservations_are_not_public_children(placement, reserved_type):
    database, pool, collections = placement
    collection = await database.find_collection(collections[0])
    collection.update(type=reserved_type, parent_ids=["reserved-parent"])
    await database.client.index(
        index=COLLECTIONS_INDEX, id=collections[0], body=collection, refresh=True
    )
    rows, matched, token = await database.get_catalog_children(
        "reserved-parent", 10, None
    )
    assert rows == [] and matched == 0 and token is None
    rows, token, matched = await database.get_all_collections(
        None, 10, None, query={"id": {"eq": collections[0]}}
    )
    assert rows == [] and token is None and matched == 0


@pytest.mark.asyncio
async def test_old_schema_writer_cannot_change_recreated_collection(placement):
    database, pool, collections = placement
    collection = await database.find_collection(collections[0])
    old_schema = await read_pool_schema(database.client, collections[0])
    await database.delete_collection(collections[0])
    await database.create_collection(collection, refresh=True)
    with pytest.raises(Exception) as stale:
        await record_pool_fields(database.client, old_schema, [{"old_only": "stale"}])
    assert stale.value.status_code == 400
    current = await read_pool_schema(database.client, collections[0])
    assert current["generation"] != old_schema["generation"]
    assert "old_only" not in current["fields"]


@pytest.mark.asyncio
async def test_inflight_write_cannot_enter_recreated_collection(
    placement, test_item, monkeypatch
):
    database, pool, collections = placement
    item = copy.deepcopy(test_item)
    item.update(id="late|item", collection=collections[0])
    collection = await database.find_collection(collections[0])
    reached, resume = asyncio.Event(), asyncio.Event()
    original = database.client.index

    async def delayed(**kwargs):
        if kwargs.get("id") == mk_item_id(item["id"], collections[0]):
            reached.set()
            await resume.wait()
        return await original(**kwargs)

    monkeypatch.setattr(database.client, "index", delayed)
    writing = asyncio.create_task(database.create_item(item, refresh=True))
    try:
        await asyncio.wait_for(reached.wait(), 5)
        await database.delete_collection(collections[0])
        await database.create_collection(collection, refresh=True)
    finally:
        resume.set()
        with pytest.raises(Exception) as stale:
            await writing
    assert stale.value.status_code in (400, 404)
    assert (await database.client.count(index=pool))["count"] == 0


@pytest.mark.asyncio
async def test_pool_deletion_keeps_other_collection_and_blocks_late_writes(placement):
    database, pool, collections = placement
    for collection in collections:
        await database.bulk_async(
            collection, [{"id": "shared", "collection": collection}], refresh=True
        )
    _, stale = await database.find_collection_with_version(collections[0])
    metadata = await database.find_collection(collections[0])
    metadata["description"] = "Updated"
    await database.update_collection(collections[0], metadata)
    with pytest.raises(HTTPException) as error:
        await database.delete_collection(collections[0], version=stale)
    assert error.value.status_code == 412
    assert (await database.client.count(index=pool))["count"] == 2
    await database.delete_collection(collections[0])
    assert (await database.client.count(index=pool))["count"] == 1
    assert await database._check_item_exists_in_collection(collections[1], "shared")
    with pytest.raises(Exception) as missing:
        await database.bulk_async(
            collections[0], [{"id": "late", "collection": collections[0]}]
        )
    assert missing.value.status_code == 404
    assert not await database.client.exists(index=CONTROL_INDEX, id=pool)
    assert not await database.client.exists(index=COLLECTIONS_INDEX, id=collections[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
async def test_pool_deletion_refreshes_drained_writes(placement, existing, monkeypatch):
    database, pool, (left, right) = placement
    await database.client.indices.put_settings(
        index=pool, body={"index.refresh_interval": "-1"}
    )
    item = {"id": "recent", "collection": left}
    await database.bulk_async(right, [{**item, "collection": right}], refresh=True)
    if existing:
        await database.bulk_async(left, [item], refresh=True)
    monkeypatch.setenv("DATABASE_REFRESH", "false")
    await database.bulk_async(
        left, [{**item, "title": "Changed"}], op_type="index", refresh=False
    )
    await database.delete_collection(left)
    assert (await database.client.count(index=pool))["count"] == 1
    assert await database._check_item_exists_in_collection(right, "recent")
    assert not await database.client.exists(index=CONTROL_INDEX, id=pool)
    assert not await database.client.exists(index=COLLECTIONS_INDEX, id=left)


@pytest.mark.asyncio
async def test_pool_refresh_failure_keeps_write_barrier(placement, monkeypatch):
    database, pool, collections = placement
    monkeypatch.setattr(
        database.client.indices,
        "refresh",
        AsyncMock(return_value={"_shards": {"failed": 1}}),
    )
    delete = AsyncMock()
    monkeypatch.setattr(database.client, "delete_by_query", delete)
    with pytest.raises(RuntimeError, match="refresh failed"):
        await database.delete_collection(collections[0])
    delete.assert_not_awaited()
    settings = await database.client.indices.get_settings(
        index=pool, flat_settings=True
    )
    assert settings[pool]["settings"]["index.blocks.write"] == "true"
    assert await database.client.exists(index=CONTROL_INDEX, id=pool)


@pytest.mark.asyncio
async def test_pool_deletion_failure_reserves_collection_id(placement, monkeypatch):
    database, pool, collections = placement
    original = await database.find_collection(collections[0])

    async def fail(**kwargs):
        raise RuntimeError("Injected barrier failure")

    monkeypatch.setattr(database.client.indices, "add_block", fail)
    with pytest.raises(RuntimeError, match="Injected"):
        await database.delete_collection(collections[0])
    assert await database.client.exists(index=CONTROL_INDEX, id=pool)
    with pytest.raises(ConflictError):
        await database.create_collection(original)
    with pytest.raises(NotFoundError):
        await database.update_collection(collections[0], original)
    operation = TypeAdapter(PatchOperation).validate_python(
        {
            "op": "replace",
            "path": "/description",
            "value": "Must not overwrite reservation",
        }
    )
    with pytest.raises(NotFoundError):
        await database.json_patch_collection(collections[0], [operation], "")
    assert (await database.client.get(index=COLLECTIONS_INDEX, id=collections[0]))[
        "_source"
    ]["type"] == "DeletingCollection"
    with pytest.raises(HTTPException) as error:
        await database.delete_collection(collections[1])
    assert error.value.status_code == 409
