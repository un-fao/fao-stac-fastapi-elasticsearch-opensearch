"""Immutable, operator-owned collection placement for pooled storage."""

import hashlib
import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database.index import active_items_index
from stac_fastapi.sfeos_helpers.mappings import ITEMS_ALIAS_PREFIX

STORAGE_NAMESPACE = (
    ""
    if ITEMS_ALIAS_PREFIX == "items_"
    else "_" + hashlib.sha256(ITEMS_ALIAS_PREFIX.encode()).hexdigest()
)


@dataclass(frozen=True)
class Pool:
    """One pre-created physical index and its routing policy."""

    index: str
    routing: bool


@dataclass(frozen=True)
class StorageManifest:
    """Placement is independent of STAC metadata and catalog membership."""

    pools: Mapping[str, Pool]
    collections: Mapping[str, str]


@lru_cache(maxsize=1)
def load_storage_manifest(path: str) -> StorageManifest:
    """Load a versioned manifest once; a placement change requires a new revision."""
    if not Path(path).is_absolute():
        raise ValueError("STAC_FASTAPI_STORAGE_MANIFEST must be an absolute file path")
    with open(path) as source:
        data = json.load(source)
    if not isinstance(data, dict) or set(data) != {"version", "pools", "collections"}:
        raise ValueError("Invalid storage manifest fields")
    if (
        type(data["version"]) is not int
        or data["version"] != 1
        or not isinstance(data["pools"], dict)
        or not isinstance(data["collections"], dict)
    ):
        raise ValueError("Invalid storage manifest version or assignments")
    pools = {}
    for name, config in data["pools"].items():
        if (
            not name
            or not isinstance(config, dict)
            or set(config) != {"index", "routing"}
            or not isinstance(config["index"], str)
            or not re.fullmatch(r"stac_pool_[a-z0-9][a-z0-9_-]*", config["index"])
            or len(config["index"]) > 255
            or type(config["routing"]) is not bool
        ):
            raise ValueError("Invalid pool definition")
        pools[name] = Pool(**config)
    if len({p.index for p in pools.values()}) != len(pools):
        raise ValueError("Each pool must have its own physical index")
    if any(
        not collection or not isinstance(group, str) or group not in pools
        for collection, group in data["collections"].items()
    ):
        raise ValueError("Collection refers to an unknown pool")
    return StorageManifest(
        MappingProxyType(pools), MappingProxyType(data["collections"])
    )


def storage_manifest() -> StorageManifest | None:
    """Return the configured placement; absence preserves dedicated storage."""
    path = os.getenv("STAC_FASTAPI_STORAGE_MANIFEST")
    if path is None:
        return None
    if not os.getenv("STAC_FASTAPI_ACTIVE_ITEMS_ALIAS"):
        raise ValueError("Pooled storage requires an active items alias")
    active_items_index()
    if get_bool_env("ENABLE_DATETIME_INDEX_FILTERING", default="false"):
        raise ValueError("Pooled storage cannot use datetime partitioning")
    if get_bool_env("ENABLE_REDIS_QUEUE", default="false"):
        raise ValueError(
            "Pooled storage requires direct writes; queued generations are unsupported"
        )
    return load_storage_manifest(path)


def collection_pool(collection_id: str) -> Pool | None:
    """Resolve a server-owned assignment without reading public collection fields."""
    manifest = storage_manifest()
    group = manifest.collections.get(collection_id) if manifest else None
    return manifest.pools[group] if manifest and group is not None else None


def storage_routing(collection_id: str) -> dict[str, str]:
    """Use one routing value even when the public collection ID contains commas."""
    pool = collection_pool(collection_id)
    return (
        {"routing": hashlib.sha256(collection_id.encode()).hexdigest()}
        if pool and pool.routing
        else {}
    )


def storage_alias(collection_id: str) -> str:
    """Keep distinct public collection IDs distinct after index-name normalization."""
    return (
        "stac_collection"
        + STORAGE_NAMESPACE
        + "_"
        + hashlib.sha256(collection_id.encode()).hexdigest()
    )


def pooled_write_options(collection_id: str) -> dict[str, bool]:
    """Require a live alias so removed collections cannot recreate physical indexes."""
    return {"require_alias": True} if collection_pool(collection_id) else {}
