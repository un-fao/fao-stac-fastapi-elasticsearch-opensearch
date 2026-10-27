"""Unit tests for bulk transaction error serialization."""

import pytest

from stac_fastapi.core.utilities import format_bulk_errors, format_conflict_errors
from stac_fastapi.sfeos_helpers.database import bulk_item_id_map


def test_format_bulk_errors_preserves_item_id_and_reason():
    errors = [
        {
            "create": {
                "_id": "item-1|collection-1",
                "status": 409,
                "error": {"type": "conflict", "reason": "already exists"},
            }
        },
        {
            "index": {
                "_id": "item-2|collection-1",
                "status": 400,
                "error": {"type": "mapper", "reason": "invalid field"},
            }
        },
    ]

    item_ids = bulk_item_id_map(
        [
            {"id": "item-1", "collection": "collection-1"},
            {"id": "item-2", "collection": "collection-1"},
        ]
    )
    assert format_bulk_errors(errors, item_ids) == [
        {"id": "item-1", "msg": "already exists"},
        {"id": "item-2", "msg": "invalid field"},
    ]


@pytest.mark.parametrize("physical_id", ["a|b|c|d", "opaque-storage-digest"])
def test_bulk_errors_use_request_identity(physical_id):
    errors = [{"create": {"_id": physical_id, "error": {"reason": "conflict"}}}]
    item_ids = {physical_id: ("a|b", "c|d")}
    assert format_bulk_errors(errors, item_ids) == [{"id": "a|b", "msg": "conflict"}]
    assert format_conflict_errors(errors, item_ids) == {
        "a|b": "Item 'a|b' already exists in collection 'c|d'"
    }


def test_bulk_errors_reject_unknown_physical_id():
    with pytest.raises(KeyError):
        format_bulk_errors([{"create": {"_id": "unknown"}}], {})
