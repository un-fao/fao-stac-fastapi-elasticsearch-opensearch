"""Isolated pooled collections on disposable test backends."""

import copy
import json
import uuid

import pytest_asyncio

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.database.pool_schema import CONTROL_INDEX
from stac_fastapi.sfeos_helpers.database.storage import load_storage_manifest
from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
)

from ..conftest import DatabaseLogic, create_collection_index


@pytest_asyncio.fixture
async def placement(tmp_path, monkeypatch, test_collection):
    await create_collection_index()
    suffix = uuid.uuid4().hex
    pool = "stac_pool_" + suffix
    collections = ["left," + suffix, "left|" + suffix]
    path = tmp_path / "placement.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "pools": {"test": {"index": pool, "routing": True}},
                "collections": dict.fromkeys(collections, "test"),
            }
        )
    )
    monkeypatch.setenv("STAC_FASTAPI_STORAGE_MANIFEST", str(path))
    monkeypatch.setenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS", "active-" + suffix)
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    database = DatabaseLogic()
    try:
        if not await database.client.indices.exists(index=CONTROL_INDEX):
            await database.client.indices.create(index=CONTROL_INDEX)
        await database.client.indices.create(
            index=pool,
            body={
                "settings": {
                    **ES_ITEMS_SETTINGS,
                    "number_of_shards": 4,
                    "number_of_replicas": 0,
                },
                "mappings": ES_ITEMS_MAPPINGS,
            },
        )
        await database.client.indices.put_alias(index=pool, name="active-" + suffix)
        for collection in collections:
            metadata = copy.deepcopy(test_collection)
            metadata["id"] = collection
            await database.create_collection(metadata, refresh=True)
        yield database, pool, collections
    finally:
        if await database.client.exists(index=CONTROL_INDEX, id=pool):
            await database.client.delete(index=CONTROL_INDEX, id=pool, refresh=True)
        for collection in collections:
            if await database.client.exists(index=COLLECTIONS_INDEX, id=collection):
                await database.client.delete(
                    index=COLLECTIONS_INDEX, id=collection, refresh=True
                )
            alias = index_alias_by_collection_id(collection)
            if await database.client.exists(index=CONTROL_INDEX, id=alias):
                await database.client.delete(index=CONTROL_INDEX, id=alias)
            if not await database.client.indices.exists_alias(
                name=alias
            ) and await database.client.indices.exists(index=alias):
                await database.client.indices.delete(index=alias)
        await database.client.indices.delete(index=pool)
        await database.client.close()
        database.sync_client.close()
        load_storage_manifest.cache_clear()
