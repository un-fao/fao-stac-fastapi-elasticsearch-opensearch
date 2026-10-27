"""Keep collection queryables independent of the physical pool's mapping union."""

from copy import deepcopy
from typing import Any
from uuid import uuid4

from .storage import STORAGE_NAMESPACE, collection_pool, storage_alias

CONTROL_INDEX = "stac_storage_control" + STORAGE_NAMESPACE


def pool_write_alias(schema: dict) -> str:
    """Reject writes prepared for a previous collection with the same public ID."""
    return storage_alias(schema["collection"]) + "_" + schema["generation"]


def mapping_fields(properties: dict, prefix: str = "") -> set[str]:
    """Include mapped fields even when no current document contains a value."""
    fields = set()
    for name, definition in properties.items():
        path = prefix + name
        fields.add(path)
        fields.update(mapping_fields(definition.get("properties", {}), path + "."))
    return fields


def source_fields(value: Any, prefix: str = "") -> set[str]:
    """Collect object paths without treating array positions as mapped fields."""
    fields = set()
    if isinstance(value, list):
        for item in value:
            fields.update(source_fields(item, prefix))
    elif isinstance(value, dict):
        for name, item in value.items():
            path = prefix + name
            if item is not None and item != []:
                fields.add(path)
                fields.update(source_fields(item, path + "."))
    return fields


async def seed_pool_schema(client: Any, collection_id: str, mapping: dict) -> None:
    """Seed original mappings during migration, or the template for a new collection."""
    pool = collection_pool(collection_id)
    if pool is None:
        return
    if not await client.indices.exists(index=CONTROL_INDEX):
        raise RuntimeError("Pooled storage control index is missing")
    try:
        await client.index(
            index=CONTROL_INDEX,
            id=storage_alias(collection_id),
            body={
                "collection": collection_id,
                "pool": pool.index,
                "generation": uuid4().hex,
                "fields": sorted(mapping_fields(mapping.get("properties", {}))),
            },
            op_type="create",
            refresh=True,
        )
    except Exception as error:
        if getattr(error, "status_code", None) != 409:
            raise
        await read_pool_schema(client, collection_id)


def validate_schema(collection_id: str, result: dict) -> dict:
    """Reject a stale assignment instead of returning another collection's fields."""
    schema = result["_source"]
    pool = collection_pool(collection_id)
    if (
        pool is None
        or schema["collection"] != collection_id
        or schema["pool"] != pool.index
        or not isinstance(schema["fields"], list)
        or not schema["generation"]
    ):
        raise RuntimeError("Collection schema does not match its storage assignment")
    return schema


async def read_pool_schema(client: Any, collection_id: str) -> dict | None:
    """Read the generation before a write, including before a possible deletion."""
    if collection_pool(collection_id) is None:
        return None
    result = await client.get(index=CONTROL_INDEX, id=storage_alias(collection_id))
    return validate_schema(collection_id, result)


def read_pool_schema_sync(client: Any, collection_id: str) -> dict | None:
    """Read the same schema for synchronous bulk ingestion."""
    if collection_pool(collection_id) is None:
        return None
    result = client.get(index=CONTROL_INDEX, id=storage_alias(collection_id))
    return validate_schema(collection_id, result)


def schema_update(schema: dict, items: list[dict]) -> dict | None:
    """Merge only newly observed fields, preserving concurrent additions."""
    fields = set().union(*(source_fields(item) for item in items))
    fields.difference_update(schema["fields"])
    if not fields:
        return None
    return {
        "index": CONTROL_INDEX,
        "id": storage_alias(schema["collection"]),
        "retry_on_conflict": 5,
        "body": {
            "script": {
                "lang": "painless",
                "source": """
                    if (ctx._source.generation != params.generation) {
                        throw new IllegalStateException('Collection generation changed');
                    }
                    for (field in params.fields) {
                        if (!ctx._source.fields.contains(field)) {
                            ctx._source.fields.add(field);
                        }
                    }
                """,
                "params": {
                    "generation": schema["generation"],
                    "fields": sorted(fields),
                },
            }
        },
    }


def stored_items(result: dict) -> list[dict]:
    """Use stored sources after partial failure, including duplicate IDs in one batch."""
    if any("error" in doc for doc in result["docs"]):
        raise RuntimeError("Cannot reconcile pooled queryables after bulk errors")
    return [doc["_source"] for doc in result["docs"] if doc.get("found")]


async def record_pool_fields(
    client: Any, schema: dict | None, items: list[dict], errors: bool = False
) -> None:
    """Record fields from acknowledged documents, never from rejected input."""
    if schema is None:
        return
    if errors:
        from .document import mk_item_id

        result = await client.mget(
            index=storage_alias(schema["collection"]),
            body={
                "ids": list(
                    {mk_item_id(item["id"], schema["collection"]) for item in items}
                )
            },
        )
        items = stored_items(result)
    update = schema_update(schema, items)
    if update:
        await client.update(**update)


def record_pool_fields_sync(
    client: Any, schema: dict | None, items: list[dict], errors: bool = False
) -> None:
    """Record fields for synchronous bulk responses with the same failure handling."""
    if schema is None:
        return
    if errors:
        from .document import mk_item_id

        result = client.mget(
            index=storage_alias(schema["collection"]),
            body={
                "ids": list(
                    {mk_item_id(item["id"], schema["collection"]) for item in items}
                )
            },
        )
        items = stored_items(result)
    update = schema_update(schema, items)
    if update:
        client.update(**update)


def scoped_mapping(mapping: dict, fields: list[str]) -> dict:
    """Prune the public mapping view without changing physical mappings or field types."""
    allowed = set(fields)

    def prune(properties, prefix=""):
        result = {}
        for name, definition in properties.items():
            path = prefix + name
            if path in allowed:
                result[name] = deepcopy(definition)
                if "properties" in definition:
                    result[name]["properties"] = prune(
                        definition["properties"], path + "."
                    )
        return result

    result = deepcopy(mapping)
    for value in result.values():
        value["mappings"]["properties"] = prune(value["mappings"].get("properties", {}))
    return result
