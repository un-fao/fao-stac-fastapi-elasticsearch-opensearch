"""Collection aliases and field registries keep shared storage scoped."""

import asyncio

import pytest

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.database.pool_schema import (
    read_pool_schema,
    record_pool_fields,
    scoped_mapping,
    seed_pool_schema,
    source_fields,
)


@pytest.mark.asyncio
async def test_pool_aliases_route_duplicate_public_ids(placement):
    database, pool, collections = placement
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
        assert count == 1 and not errors
    assert (await database.client.count(index=pool))["count"] == 2
    for collection in collections:
        alias = index_alias_by_collection_id(collection)
        assert (await database.client.count(index=alias))["count"] == 1
        doc = await database.client.get(index=alias, id=mk_item_id("a|b", collection))
        assert doc["_source"]["collection"] == collection


@pytest.mark.asyncio
async def test_existing_alias_is_never_silently_reassigned(placement):
    database, pool, collections = placement
    alias = index_alias_by_collection_id(collections[0])
    await database.client.indices.put_alias(
        index=pool,
        name=alias,
        body={"filter": {"term": {"collection": collections[1]}}},
    )
    with pytest.raises(RuntimeError, match="does not match"):
        await database.async_index_inserter.create_simple_index(
            database.client, collections[0]
        )
    actual = await database.client.indices.get_alias(name=alias)
    assert (
        actual[pool]["aliases"][alias]["filter"]["term"]["collection"] == collections[1]
    )


@pytest.mark.asyncio
async def test_field_registry_merges_concurrent_additions_without_reseeding(placement):
    database, pool, collections = placement
    collection = collections[0]
    schema = await read_pool_schema(database.client, collection)
    await asyncio.gather(
        record_pool_fields(database.client, schema, [{"custom": {"left": 1}}]),
        record_pool_fields(database.client, schema, [{"custom": {"right": 2}}]),
    )
    current = await read_pool_schema(database.client, collection)
    assert {"custom", "custom.left", "custom.right"}.issubset(current["fields"])
    await seed_pool_schema(
        database.client, collection, {"properties": {"reset": {"type": "long"}}}
    )
    assert await read_pool_schema(database.client, collection) == current
    assert (
        "custom"
        not in (await read_pool_schema(database.client, collections[1]))["fields"]
    )


def test_scoped_mapping_retains_field_types_and_multifields():
    field = {"type": "text", "fields": {"raw": {"type": "keyword"}}}
    original = {
        "pool": {"mappings": {"properties": {"a": field, "b": {"type": "long"}}}}
    }
    assert scoped_mapping(original, ["a"])["pool"]["mappings"]["properties"] == {
        "a": field
    }
    assert "b" in original["pool"]["mappings"]["properties"]
    assert source_fields(
        {"array": [{"left": 1}, {"right": 2}], "null": None, "empty": []}
    ) == {"array", "array.left", "array.right"}
