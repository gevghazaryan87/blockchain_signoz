"""Abstract base class for blockchain data providers."""
from abc import ABC, abstractmethod


class BlockchainProvider(ABC):
    """Interface that all blockchain data sources must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name of this provider (e.g. 'blockstream')."""
        ...

    @property
    def rate_limit(self) -> int:
        """Requests per hour limit (default: 3600 = 1/sec)."""
        return 3600

    @abstractmethod
    def get_latest_blocks(self, count: int = 10) -> list[dict]:
        """Return the latest *count* blocks."""
        ...

    @abstractmethod
    def get_block_transactions(self, block_hash: str, start_index: int = 0) -> list[dict] | None:
        """Return a batch of transactions for a given block starting at *start_index*."""
        ...
