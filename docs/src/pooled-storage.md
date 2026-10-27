# Optional pooled storage

Pooled storage shares an item index between explicitly assigned collections. Public collection and item IDs stay unchanged. Catalog membership does not determine placement: linking, unlinking or reparenting a collection leaves its items in place. Collections absent from the manifest retain dedicated storage.

This is an operator-managed migration feature. It does not create pools, copy existing items or switch aliases automatically. Enable it only after verifying the target data and recovery procedure on a disposable deployment. Redis queuing and datetime partitioning are unsupported with a pooled manifest; startup rejects those combinations.

## Configuration and preparation

Set `STAC_FASTAPI_STORAGE_MANIFEST` to an absolute, readable JSON file path. The process caches that file; publish a new immutable version and restart the application for each placement change. Do not edit a mounted file under running instances. Also set `STAC_FASTAPI_ACTIVE_ITEMS_ALIAS` to one explicit alias containing only the active item indexes.

```json
{
  "version": 1,
  "pools": {
    "rainfall": {"index": "stac_pool_rainfall_v1", "routing": true}
  },
  "collections": {"monthly-rainfall": "rainfall"}
}
```

Create each physical pool with the intended mappings, shard count, replicas and physical sort. Preserve every required field and type; multiple primary shards do not raise an index's field limit. Create the private `stac_storage_control` index, and include it in backup and replica planning along with collection/catalog metadata. It contains field registries and operation reservations, not public STAC resources.

For an existing collection, pause writes, copy its items using `mk_item_id` and the configured collection routing, and verify counts and content. Seed its private field registry with `seed_pool_schema` using the original item mapping, then attach its filtered read and generation-specific write aliases with `PooledIndexInserter.create_simple_index`. Seed before attaching aliases so queryables retain fields from the original mapping even if current items do not contain them. Keep explicit source-to-target mappings and original settings for rollback.

Switch the active alias and application placement only within a quiescent cutover. Until both agree, serving requests can expose duplicates or target different storage. Startup verifies pool membership, collection state, field registries and exact collection-alias filters/routing. New assigned collections get their aliases and field registry when created. Placement is kept outside public metadata; PUT/PATCH cannot change the manifest.

Reindexing changes document versions. Existing ETags may fail `If-Match`, and pagination tokens must be restarted at the cutover boundary. Retain sources and verified snapshots until rollback retention expires. Rolling back after writes requires reconciling authoritative active content, including deletions, into the old storage before moving readers.

## Interrupted collection operations

Collection deletion briefly blocks writes to its shared pool to drain earlier requests. Other collections' documents remain intact, but concurrent writes may receive a retryable service error. The operation removes the selected collection's aliases and documents; it never deletes the physical pool.

A failed creation retains `CreatingCollection` metadata. A failed deletion retains `DeletingCollection` metadata and a pool-level lock in `stac_storage_control`. Such records are excluded from public listings. Do not clear a reservation just to make startup pass: it prevents old in-flight writes from entering a new collection with the same ID.

Before operator recovery, stop and drain all application and direct writers to the affected pool. Save the current metadata, private schema/lock documents, aliases, settings and item content outside Elasticsearch. Preserve a verified recovery point and use sequence-number/primary-term checks for metadata/control changes.

For an interrupted creation, verify the manifest, active pool membership, private schema and both aliases. Preserve any existing items. Complete initialization, then change the reserved metadata back to `Collection` with a conditional write. Do not generate a replacement schema over an existing registry.

For an interrupted deletion, keep the collection reserved. Remove its exact remaining read/write aliases, acknowledge the pool write barrier and confirm earlier writes are drained. Clear only the write block owned by this operation. Delete only documents matching that collection, with its configured routing; verify zero failures/conflicts and zero remaining documents. Then conditionally remove its private schema and reserved metadata, and release the saved pool lock last. If ownership, content or operation state is uncertain, remain paused and reconcile from the saved recovery evidence instead of guessing.

After recovery, validate placement, verify neighboring collections and acknowledged writes, and rerun startup checks before resuming writers. The public DELETE endpoint does not resume a failed operation automatically.
