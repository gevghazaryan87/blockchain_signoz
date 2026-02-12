"""Sandshrew (Esplora via JSON-RPC) data provider."""
import requests
from extraction.base import BlockchainProvider
from config import HEADERS

class SandshrewProvider(BlockchainProvider):
    """Fetches block and transaction data from sandshrew.io via JSON-RPC."""

    # Using the user-provided API key
    RPC_URL = "https://mainnet.sandshrew.io/v1/f175065177da3cab899fe8acf255ecd5"

    @property
    def name(self) -> str:
        return "sandshrew"

    @property
    def rate_limit(self) -> int:
        # Sandshrew has a high cap (100k/day), so we can be generous
        return 20000

    def _rpc_request(self, method: str, params: list = None) -> any:
        """Helper to make a JSON-RPC request."""
        if params is None:
            params = []
            
        payload = {
            "jsonrpc": "2.0",
            "id": "antigravity",
            "method": method,
            "params": params
        }
        
        try:
            response = requests.post(self.RPC_URL, json=payload, headers=HEADERS, timeout=45)
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                print(f"\n   ❌ Sandshrew RPC Error [{method}]: {data['error']}")
                return None
                
            return data.get("result")
        except Exception as e:
            print(f"\n   ❌ Sandshrew Request Error [{method}]: {e}")
            return None

    def get_latest_blocks(self, count: int = 10) -> list[dict]:
        # esplora_blocks maps to GET /blocks
        # It accepts an optional start_height, but we just want latest.
        # The API returns the latest 10 blocks by default.
        blocks = self._rpc_request("esplora_blocks")
        if not blocks:
            return []
        return blocks[:count]

    def get_block_transactions(self, block_hash: str, start_index: int = 0) -> list[dict] | None:
        # esplora_block::txs maps to GET /block/:hash/txs/:start_index
        # Param 1: block hash
        # Param 2: start index
        return self._rpc_request("esplora_block::txs", [block_hash, str(start_index)])
