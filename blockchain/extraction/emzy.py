"""Emzy (Esplora) data provider."""
from extraction.base import BlockchainProvider
from api_client import get_api_data


class EmzyProvider(BlockchainProvider):
    """Fetches block and transaction data from mempool.emzy.de/api."""

    BASE_URL = "https://mempool.emzy.de/api"

    @property
    def name(self) -> str:
        return "emzy"

    @property
    def rate_limit(self) -> int:
        return 600

    def get_latest_blocks(self, count: int = 10) -> list[dict]:
        blocks = get_api_data(f"{self.BASE_URL}/blocks")
        if not blocks:
            return []
        return blocks[:count]

    def get_block_transactions(self, block_hash: str, start_index: int = 0) -> list[dict] | None:
        url = f"{self.BASE_URL}/block/{block_hash}/txs/{start_index}"
        return get_api_data(url)
