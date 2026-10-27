"""Public bulk identities must not depend on storage ID encoding."""

from unittest.mock import AsyncMock, Mock

import pytest

from stac_fastapi.core.core import BulkTransactionsClient, TransactionsClient
from stac_fastapi.extensions.bulk_transactions import Items
from stac_fastapi.sfeos_helpers.database import ItemAlreadyExistsError, document


@pytest.fixture(params=[False, True], ids=["delimiter", "opaque"])
def batch(monkeypatch, request):
    for key in (
        "ENABLE_REDIS_QUEUE",
        "ENABLE_STAC_VALIDATOR",
        "ENABLE_DATETIME_INDEX_FILTERING",
    ):
        monkeypatch.setenv(key, "false")
    monkeypatch.setenv("MAX_BATCH_SIZE", "0")
    monkeypatch.setattr(
        BulkTransactionsClient, "preprocess_item", lambda self, item, base_url: item
    )
    if request.param:
        monkeypatch.setattr(
            document,
            "mk_item_id",
            lambda item, collection: "opaque-" + item.encode().hex(),
        )
    features = [{"id": item, "collection": "collection|part"} for item in ("a|b", "a")]
    error = {
        "create": {
            "_id": document.mk_item_id("a|b", "collection|part"),
            "status": 409,
            "error": {"type": "conflict", "reason": "already exists"},
        }
    }
    return features, error


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
async def test_async_bulk_does_not_report_failed_item_as_success(
    batch, monkeypatch, strict
):
    features, error = batch
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", str(strict).lower())
    database = Mock(bulk_async=AsyncMock(return_value=(1, [error])))
    client = TransactionsClient(database=database, session=None, settings=Mock())
    monkeypatch.setattr(
        client, "_validate_feature_collection", AsyncMock(return_value=(features, {}))
    )
    operation = client._create_feature_collection(
        "collection|part",
        {"type": "FeatureCollection", "features": features},
        "http://test-server/",
        False,
    )
    if strict:
        with pytest.raises(ItemAlreadyExistsError, match=r"a\|b"):
            await operation
    else:
        result = await operation
        assert result["successfully_added"] == ["a"]
        assert result["conflict_errors"] == {
            "a|b": "Item 'a|b' already exists in collection 'collection|part'"
        }


@pytest.mark.parametrize("strict", [False, True])
def test_sync_bulk_reports_public_item_id(batch, monkeypatch, strict):
    features, error = batch
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", str(strict).lower())
    database = Mock(bulk_sync=Mock(return_value=(1, [error])))
    client = BulkTransactionsClient(database=database, settings=Mock())
    items = Items(items={item["id"]: item for item in features})
    if strict:
        with pytest.raises(ItemAlreadyExistsError, match=r"a\|b"):
            client.bulk_item_insert(items)
    else:
        result = client.bulk_item_insert(items)
        assert result["success"] == 1
        assert result["errors"] == [{"id": "a|b", "msg": "already exists"}]
