"""Public pooled searches preserve scope and startup rejects incomplete placement."""

import copy
import os
import uuid
from urllib.parse import quote

import pytest
from httpx import ASGITransport, AsyncClient

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.database.pool_schema import (
    pool_write_alias,
    read_pool_schema,
)
from stac_fastapi.sfeos_helpers.database.pool_startup import validate_pool_storage
from stac_fastapi.sfeos_helpers.database.storage import storage_manifest


@pytest.mark.asyncio
async def test_existing_wrong_alias_is_not_reassigned(placement):
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
    with pytest.raises(RuntimeError, match="aliases do not match"):
        await validate_pool_storage(database.client)


@pytest.mark.asyncio
async def test_startup_requires_active_pool_and_complete_aliases(placement):
    database, pool, collections = placement
    await validate_pool_storage(database.client)
    schema = await read_pool_schema(database.client, collections[0])
    await database.client.indices.delete_alias(
        index=pool, name=pool_write_alias(schema)
    )
    with pytest.raises(RuntimeError, match="aliases do not match"):
        await validate_pool_storage(database.client)


@pytest.mark.asyncio
async def test_pooled_http_identity_queryables_and_collection_deletion(
    placement, test_item
):
    database, pool, collections = placement
    if os.getenv("BACKEND", "elasticsearch") == "opensearch":
        from stac_fastapi.opensearch.app import instantiate_api
    else:
        from stac_fastapi.elasticsearch.app import instantiate_api
    app = instantiate_api(database_logic=database).app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        for number, collection in enumerate(collections):
            item = copy.deepcopy(test_item)
            item.update(id="same|public-id", collection=collection)
            item["properties"]["only_" + str(number)] = number
            result = await client.post(
                f"/collections/{quote(collection)}/items", json=item
            )
            assert result.status_code == 201, result.text
        for number, collection in enumerate(collections):
            path = f"/collections/{quote(collection)}"
            result = await client.get(path + "/items/same%7Cpublic-id")
            assert result.status_code == 200
            assert result.json()["collection"] == collection
            fields = (await client.get(path + "/queryables")).json()["properties"]
            assert "only_" + str(number) in fields
            assert "only_" + str(1 - number) not in fields
            result = await client.post("/search", json={"collections": [collection]})
            assert result.status_code == 200
            assert [
                (item["collection"], item["id"]) for item in result.json()["features"]
            ] == [(collection, "same|public-id")]
            result = await client.post(
                "/search",
                json={
                    "filter-lang": "cql2-json",
                    "filter": {
                        "op": "=",
                        "args": [{"property": "collection"}, collection],
                    },
                },
            )
            assert result.status_code == 200, result.text
            assert [item["collection"] for item in result.json()["features"]] == [
                collection
            ]
            result = await client.post(
                "/aggregate",
                json={"collections": [collection], "aggregations": ["total_count"]},
            )
            assert result.status_code == 200, result.text
            assert result.json()["aggregations"][0]["value"] == 1
        result = await client.post("/search", json={"limit": 100})
        assert result.status_code == 200 and len(result.json()["features"]) == 2
        result = await client.delete(f"/collections/{quote(collections[0])}")
        assert result.status_code == 204, result.text
        result = await client.get(
            f"/collections/{quote(collections[1])}/items/same%7Cpublic-id"
        )
        assert result.status_code == 200
        result = await client.post("/search", json={"limit": 100})
        assert result.status_code == 200 and len(result.json()["features"]) == 1


@pytest.mark.asyncio
async def test_pool_queryables_follow_successful_writes_only(placement, test_item):
    database, pool, (left, right) = placement
    item = copy.deepcopy(test_item)
    item.update(id="left|ok", collection=left)
    item["properties"]["left_only"] = {"leaf": 1}
    failed = copy.deepcopy(item)
    failed["id"] = "left|bad"
    failed["properties"]["eo:cloud_cover"] = {"invalid": True}
    failed["properties"]["failed_only"] = "not indexed"
    count, errors = await database.bulk_async(left, [item, failed], refresh=True)
    assert count == 1 and len(errors) == 1
    fields = await database.get_queryables_mapping(left)
    assert fields["left_only.leaf"] == ["properties.left_only.leaf"]
    assert "failed_only" not in fields
    assert (
        "properties.failed_only"
        not in (await read_pool_schema(database.client, left))["fields"]
    )
    assert "left_only.leaf" not in await database.get_queryables_mapping(right)

    other = copy.deepcopy(test_item)
    other.update(id="right|ok", collection=right)
    other["properties"]["right_only"] = 2
    count, errors = database.bulk_sync(right, [other], refresh=True)
    assert count == 1 and not errors
    assert "right_only" in await database.get_queryables_mapping(right)
    assert "right_only" not in await database.get_queryables_mapping(left)
    await database.delete_item(item["id"], left, refresh=True)
    assert "left_only.leaf" in await database.get_queryables_mapping(left)
    assert "left_only.leaf" not in await database.get_queryables_mapping(right)


@pytest.mark.asyncio
async def test_catalog_membership_and_public_metadata_cannot_move_pooled_items(
    placement, test_item, load_test_data, monkeypatch
):
    database, pool, collections = placement
    monkeypatch.setenv("ENABLE_CATALOGS_ROUTE", "true")
    backend = os.getenv("BACKEND", "elasticsearch")
    if backend == "opensearch":
        from stac_fastapi.opensearch.app import instantiate_api
    else:
        from stac_fastapi.elasticsearch.app import instantiate_api
    app = instantiate_api(database_logic=database).app
    collection = collections[0]
    path = f"/collections/{quote(collection)}"
    before = await read_pool_schema(database.client, collection)
    catalogs = ["parent-" + uuid.uuid4().hex for _ in range(2)]
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        try:
            for catalog in catalogs:
                metadata = load_test_data("test_catalog.json")
                metadata["id"] = catalog
                response = await client.post("/catalogs", json=metadata)
                assert response.status_code == 201, response.text
                response = await client.post(
                    f"/catalogs/{catalog}/collections", json={"id": collection}
                )
                assert response.status_code == 200, response.text
            items = []
            for number in range(7):
                item = copy.deepcopy(test_item)
                item.update(id=f"page|{number}", collection=collection)
                items.append(item)
            count, errors = await database.bulk_async(collection, items, refresh=True)
            assert count == 7 and not errors
            current = await client.get(path)
            metadata = current.json()
            assert pool not in current.text and before["generation"] not in current.text
            metadata["storage_group"] = "client-cannot-reassign"
            response = await client.put(path, json=metadata)
            assert response.status_code == 200, response.text
            response = await client.patch(
                path,
                json=[{"op": "add", "path": "/storage_group", "value": "ignored"}],
                headers={"Content-Type": "application/json-patch+json"},
            )
            assert response.status_code == 200, response.text
            assert set(
                (await database.find_collection(collection))["parent_ids"]
            ) == set(catalogs)
            for catalog in catalogs:
                seen, token = [], None
                while True:
                    body = {"limit": 2, **({"token": token} if token else {})}
                    response = await client.post(
                        f"/catalogs/{catalog}/search", json=body
                    )
                    assert response.status_code == 200, response.text
                    data = response.json()
                    seen.extend(
                        (item["collection"], item["id"]) for item in data["features"]
                    )
                    next_link = next(
                        (link for link in data["links"] if link["rel"] == "next"), None
                    )
                    if next_link is None:
                        break
                    token = next_link["body"]["token"]
                    assert len(seen) <= 7
                assert sorted(seen) == sorted(
                    (collection, item["id"]) for item in items
                )
            response = await client.delete(
                f"/catalogs/{catalogs[0]}/collections/{quote(collection)}"
            )
            assert response.status_code == 204, response.text
            assert (await database.find_collection(collection))["parent_ids"] == [
                catalogs[1]
            ]
            assert (await database.client.count(index=pool))["count"] == 7
            assert (
                storage_manifest()
                .pools[storage_manifest().collections[collection]]
                .index
                == pool
            )
            after = await read_pool_schema(database.client, collection)
            assert after["generation"] == before["generation"]
        finally:
            for catalog in catalogs:
                response = await client.delete(f"/catalogs/{catalog}")
                assert response.status_code in (204, 404), response.text
