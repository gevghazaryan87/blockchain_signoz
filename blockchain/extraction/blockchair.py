"""Blockchair data provider."""
import time
from extraction.base import BlockchainProvider
from api_client import get_api_data


class BlockchairProvider(BlockchainProvider):
    """Fetches block and transaction data from blockchair com."""

    # Blockchair has a different API structure, so we need to map fields carefully
    BASE_URL = "https://api.blockchair.com/bitcoin"

    @property
    def name(self) -> str:
        return "blockchair"
    
    @property
    def rate_limit(self) -> int:
        return 600  # Conservative estimate (free tier varies)

    def get_latest_blocks(self, count: int = 10) -> list[dict]:
        """Fetch latest blocks."""
        # Blockchair blocks endpoint: /bitcoin/blocks?limit=10
        data = get_api_data(f"{self.BASE_URL}/blocks?limit={count}")
        if not data or "data" not in data:
            return []
        
        # Remap Blockchair response to our standard schema
        blocks = []
        for b in data["data"]:
            blocks.append({
                "id": b["hash"],
                "height": b["id"],  # Blockchair uses 'id' for height
                "timestamp": time.mktime(time.strptime(b["time"], "%Y-%m-%d %H:%M:%S")), # "2024-01-01 12:00:00"
                "tx_count": b["transaction_count"],
                "previousblockhash": None, # Not always easily available in list view
                "version": b["version"],
                "merkle_root": b["merkle_root"],
                "bits": str(b["bits"]),
                "nonce": b["nonce"]
            })
        return blocks

    def get_block_transactions(self, block_hash: str, start_index: int = 0) -> list[dict] | None:
        """Fetch transactions for a block. 
        
        Note: This is a heavy operation for Blockchair without a key as it requires
        fetching the block dashboard first to get hashes, then fetching tx details.
        """
        # 1. Get transaction hashes from block dashboard
        # Blockchair limit for free tier is usually 100 txs per block dashboard
        block_data = get_api_data(f"{self.BASE_URL}/dashboards/block/{block_hash}")
        if not block_data or "data" not in block_data:
            return None
        
        tx_hashes = block_data["data"][block_hash].get("transactions", [])
        batch_hashes = tx_hashes[start_index:start_index + 25]
        
        if not batch_hashes:
            return []

        # 2. Fetch details for these hashes
        hashes_str = ",".join(batch_hashes)
        txs_data = get_api_data(f"{self.BASE_URL}/dashboards/transactions/{hashes_str}")
        if not txs_data or "data" not in txs_data:
            return None

        # 3. Translate Blockchair schema -> Esplora-like schema
        translated_txs = []
        for tx_hash in batch_hashes:
            if tx_hash not in txs_data["data"]:
                continue
            
            raw_tx = txs_data["data"][tx_hash]["transaction"]
            raw_vins = txs_data["data"][tx_hash].get("inputs", [])
            raw_vouts = txs_data["data"][tx_hash].get("outputs", [])
            
            # Map to our internal format (Esplora style)
            translated = {
                "txid": tx_hash,
                "version": raw_tx.get("version"),
                "locktime": raw_tx.get("locktime"),
                "size": raw_tx.get("size"),
                "weight": raw_tx.get("weight"),
                "status": {
                    "confirmed": True,
                    "block_height": raw_tx.get("block_id"),
                    "block_hash": block_hash
                },
                "vin": [
                    {
                        "txid": vin.get("spending_transaction_hash"),
                        "vout": vin.get("spending_output_index"),
                        "prevout": {
                            "value": vin.get("value"),
                            "scriptpubkey": vin.get("recipient") # Placeholder
                        },
                        "scriptsig": vin.get("script_hex"),
                        "sequence": vin.get("sequence"),
                        "is_coinbase": vin.get("is_coinbase", False)
                    } for vin in raw_vins
                ],
                "vout": [
                    {
                        "value": vout.get("value"),
                        "scriptpubkey": vout.get("script_hex"),
                        "scriptpubkey_address": vout.get("recipient")
                    } for vout in raw_vouts
                ]
            }
            translated_txs.append(translated)
            
        return translated_txs
