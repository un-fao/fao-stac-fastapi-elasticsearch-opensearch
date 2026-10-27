"""Factory for creating index insertion strategies."""

from typing import Any

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database.storage import storage_manifest

from .base import BaseIndexInserter
from .index_operations import IndexOperations
from .inserters import DatetimeIndexInserter, PooledIndexInserter, SimpleIndexInserter


class IndexInsertionFactory:
    """Factory for creating index insertion strategies."""

    @staticmethod
    def create_insertion_strategy(
        client: Any,
    ) -> BaseIndexInserter:
        """Create async insertion strategy based on configuration.

        Args:
            client: Async search engine client instance.

        Returns:
            BaseIndexInserter: Configured async insertion strategy.
        """
        index_operations = IndexOperations()

        use_datetime_partitioning = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        if storage_manifest() is not None:
            return PooledIndexInserter(index_operations, client)
        if use_datetime_partitioning:
            return DatetimeIndexInserter(client, index_operations)
        else:
            return SimpleIndexInserter(index_operations, client)
