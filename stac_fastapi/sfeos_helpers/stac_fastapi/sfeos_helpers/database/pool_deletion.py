"""Coordinate collection deletion without deleting a shared pool."""

from typing import Any

from fastapi import HTTPException

from stac_fastapi.core.etag import DocumentVersion, precondition_failed
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

from .pool_schema import CONTROL_INDEX, pool_write_alias, read_pool_schema
from .storage import collection_pool, storage_alias, storage_routing


async def delete_pooled_collection(
    client: Any, collection_id: str, version: DocumentVersion | None
) -> None:
    """Reserve the ID, drain earlier writes, then delete only its documents."""
    pool = collection_pool(collection_id)
    if pool is None:
        raise ValueError("Collection has no pool assignment")
    current = await client.get(index=COLLECTIONS_INDEX, id=collection_id)
    if current["_source"].get("type") != "Collection":
        raise HTTPException(404, "Collection not found")
    if version and DocumentVersion.from_hit(current).etag != version.etag:
        raise precondition_failed(f"Collection {collection_id}")
    if not await client.indices.exists(index=CONTROL_INDEX):
        raise RuntimeError("Pooled storage control index is missing")
    settings = await client.indices.get_settings(index=pool.index, flat_settings=True)
    if settings[pool.index]["settings"].get("index.blocks.write") == "true":
        raise HTTPException(503, "Pool is already write-blocked")
    try:
        lock = await client.index(
            index=CONTROL_INDEX,
            id=pool.index,
            body={"collection": collection_id},
            op_type="create",
            refresh=True,
        )
    except Exception as error:
        if getattr(error, "status_code", None) == 409:
            raise HTTPException(409, "Pool deletion already in progress") from error
        raise
    complete = False
    try:
        try:
            reserved = await client.index(
                index=current["_index"],
                id=collection_id,
                body={**current["_source"], "type": "DeletingCollection"},
                if_seq_no=current["_seq_no"],
                if_primary_term=current["_primary_term"],
                refresh=True,
            )
        except Exception as error:
            if getattr(error, "status_code", None) == 409:
                complete = True
                if version:
                    raise precondition_failed(f"Collection {collection_id}") from error
                raise HTTPException(
                    409, "Collection changed during deletion"
                ) from error
            raise
        schema = await read_pool_schema(client, collection_id)
        removed = await client.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"index": pool.index, "alias": name, "must_exist": True}}
                    for name in (storage_alias(collection_id), pool_write_alias(schema))
                ]
            }
        )
        if not removed.get("acknowledged"):
            raise RuntimeError("Collection alias removal was not acknowledged")
        barrier = await client.indices.add_block(index=pool.index, block="write")
        if not (
            barrier.get("acknowledged")
            and barrier.get("shards_acknowledged")
            and barrier.get("indices") == [{"name": pool.index, "blocked": True}]
        ):
            raise RuntimeError("Pool write barrier was not fully acknowledged")
        refreshed = await client.indices.refresh(index=pool.index)
        if refreshed["_shards"]["failed"]:
            raise RuntimeError("Pool refresh failed after draining writes")
        unblocked = await client.indices.put_settings(
            index=pool.index, body={"index.blocks.write": False}
        )
        if not unblocked.get("acknowledged"):
            raise RuntimeError("Pool write unblock was not acknowledged")
        query = {"term": {"collection": collection_id}}
        result = await client.delete_by_query(
            index=pool.index,
            body={"query": query},
            refresh=True,
            **storage_routing(collection_id),
        )
        if (
            result.get("timed_out")
            or result.get("failures")
            or result.get("version_conflicts")
        ):
            raise RuntimeError("Pooled collection deletion was incomplete")
        remaining = await client.count(
            index=pool.index, body={"query": query}, **storage_routing(collection_id)
        )
        if remaining["count"] or remaining["_shards"]["failed"]:
            raise RuntimeError("Pooled collection still contains items")
        await client.delete(
            index=CONTROL_INDEX, id=storage_alias(collection_id), refresh=True
        )
        await client.delete(
            index=current["_index"],
            id=collection_id,
            if_seq_no=reserved["_seq_no"],
            if_primary_term=reserved["_primary_term"],
            refresh=True,
        )
        complete = True
    finally:
        # A failed deletion keeps its reservation and lock for explicit recovery.
        if complete:
            await client.delete(
                index=CONTROL_INDEX,
                id=pool.index,
                if_seq_no=lock["_seq_no"],
                if_primary_term=lock["_primary_term"],
                refresh=True,
            )
