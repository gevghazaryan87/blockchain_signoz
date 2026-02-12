"""Blockchain.info data provider."""
import threading
from extraction.base import BlockchainProvider
from api_client import get_api_data


class BlockchainInfoProvider(BlockchainProvider):
    """Fetches data from blockchain.info."""

    BASE_URL = "https://blockchain.info"

    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return "blockchain_info"

    @property
    def rate_limit(self) -> int:
        return 1000 # Very generous usually

    def get_latest_blocks(self, count: int = 10) -> list[dict]:
        # They have a blocks endpoint but it's per day or similar
        # For simplicity, we can fetch the latest block from their stats or similar
        # But since we have other providers for headers, we can just return empty or implement basic
        # Let's use it mainly for high-speed transaction fetching
        return [] 

    def get_block_transactions(self, block_hash: str, start_index: int = 0) -> list[dict] | None:
        """Fetch and slice transactions from the full block dump."""
        with self._lock:
            if block_hash not in self._cache:
                # Fetch FULL block data
                data = get_api_data(f"{self.BASE_URL}/rawblock/{block_hash}")
                if not data or "tx" not in data:
                    return None
                self._cache[block_hash] = data["tx"]
        
        txs = self._cache[block_hash]
        batch = txs[start_index:start_index + 25]
        
        if not batch:
            return []

        # Translate to our schema
        translated = []
        for tx in batch:
            t = {
                "txid": tx.get("hash"),
                "version": tx.get("ver"),
                "locktime": tx.get("lock_time"),
                "size": tx.get("size"),
                "weight": tx.get("weight"),
                "status": {
                    "confirmed": True,
                    "block_height": tx.get("block_height"),
                    "block_hash": block_hash
                },
                "vin": [
                    {
                        "txid": None, # Blockchain.info doesn't easily show prev_txid in this view
                        "vout": vin.get("prev_out", {}).get("n"),
                        "prevout": {
                            "value": vin.get("prev_out", {}).get("value"),
                            "scriptpubkey": vin.get("prev_out", {}).get("script")
                        },
                        "scriptsig": vin.get("script"),
                        "sequence": vin.get("sequence"),
                        "is_coinbase": "prev_out" not in vin
                    } for vin in tx.get("inputs", [])
                ],
                "vout": [
                    {
                        "value": vout.get("value"),
                        "scriptpubkey": vout.get("script"),
                        "scriptpubkey_address": vout.get("addr")
                    } for vout in tx.get("out", [])
                ]
            }
            translated.append(t)
            
        return translated
