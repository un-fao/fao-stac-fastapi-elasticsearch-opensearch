"""Validate operator-created pools before an application revision accepts traffic."""

from typing import Any

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

from .index import active_items_index
from .pool_schema import CONTROL_INDEX, pool_write_alias, read_pool_schema
from .storage import storage_alias, storage_manifest, storage_routing


async def validate_pool_storage(client: Any) -> None:
    """Fail startup on incomplete placement, aliases, or recovery reservations."""
    manifest = storage_manifest()
    if manifest is None:
        return
    if not await client.indices.exists(index=CONTROL_INDEX):
        raise RuntimeError("Pooled storage control index is missing")
    active = await client.indices.get_alias(name=active_items_index())
    aliases = {}
    for pool in manifest.pools.values():
        if (
            pool.index not in active
            or active[pool.index]["aliases"][active_items_index()]
        ):
            raise RuntimeError("Pool must have unfiltered active-index membership")
        aliases[pool.index] = (await client.indices.get_alias(index=pool.index))[
            pool.index
        ]["aliases"]
        if await client.exists(index=CONTROL_INDEX, id=pool.index):
            raise RuntimeError(
                "Pool has an unfinished deletion; recover it before startup"
            )
    for collection_id, group in manifest.collections.items():
        if not await client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            if await client.exists(
                index=CONTROL_INDEX, id=storage_alias(collection_id)
            ):
                raise RuntimeError("Schema exists without collection metadata")
            continue
        metadata = await client.get(index=COLLECTIONS_INDEX, id=collection_id)
        if metadata["_source"].get("type") != "Collection":
            raise RuntimeError("Collection has an unfinished storage operation")
        schema = await read_pool_schema(client, collection_id)
        expected = {
            "filter": {"term": {"collection": collection_id}},
            "is_write_index": True,
        }
        routing = storage_routing(collection_id).get("routing")
        if routing:
            expected.update(index_routing=routing, search_routing=routing)
        pool_aliases = aliases[manifest.pools[group].index]
        if any(
            pool_aliases.get(name) != expected
            for name in (storage_alias(collection_id), pool_write_alias(schema))
        ):
            raise RuntimeError("Collection aliases do not match its storage assignment")
