import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from api_client import get_api_data
from db_operations import insert_block_header, insert_transaction_batch, is_block_fully_synced
from opentelemetry import trace

tracer = trace.get_tracer('bitcoin_ingest')

def fetch_and_store_batch(block, idx, total_txs):
    block_hash = block['id']

    with tracer.start_as_current_span(
        "bitcoin.block.txs.batch",
        attributes={
            "bitcoin.block.height": block["height"],
            "bitcoin.block.hash": block_hash,
            "batch.start_index": idx,
            "batch.size": 25,
            "block.total_txs": total_txs,
        }
    ):
    
        """Fetch and store a single batch of transactions."""
        url = f"https://blockstream.info/api/block/{block_hash}/txs/{idx}"

        with tracer.start_as_current_span(
            "bitcoin.api.fetch",
            attributes={
                "http.method": "GET",
                "http.url": url,
                "bitcoin.block.hash": block_hash,
                "batch.start_index": idx,
            }
        ):
            tx_data = get_api_data(url)

        if not tx_data:
            return 0, f"Batch starting at index {idx} failed (API limit or error)"
    
        try:

            with tracer.start_as_current_span(
                "bitcoin.db.ingest",
                attributes={
                    "bitcoin.block.hash": block_hash,
                    "batch.start_index": idx,
                    "tx.count": len(tx_data),
                },
            ):


                count = insert_transaction_batch(
                    tx_data,
                    block_hash,
                    base_index=idx
                )

            return count, None

        except Exception as e:
            return 0, f"Batch store failed at index {idx}: {e}"


def sync_full_block(block, block_pbar=None, max_workers=5, transaction_ratio_to_fetch=100):
    """Orchestrates parallel fetching and storage of all transactions in a block."""
    block_hash = block['id']
    total_txs = block['tx_count']
    txs_to_fetch = int(transaction_ratio_to_fetch * total_txs / 100)
    
    with tracer.start_as_current_span(
        "bitcoin.block.sync",
        attributes={
            "bitcoin.block.height": block["height"],
            "bitcoin.block.hash": block_hash,
            "bitcoin.block.max_workers": max_workers,
        }
    ):
        if is_block_fully_synced(block_hash, total_txs):
            if block_pbar:
                block_pbar.write(f"✅ Block #{block['height']} is already fully indexed. Skipping.")
            return True

        # 1. Store Header
        insert_block_header(block)

        # 2. Setup Pagination
        indices = list(range(0, txs_to_fetch, 25))
        total_stored = 0

        tx_pbar = tqdm(
            total=total_txs,
            desc=f"Block #{block['height']}",
            unit="tx",
            leave=True,
            position=1,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
        )
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(fetch_and_store_batch, block, idx, total_txs): idx 
                for idx in indices 
            }
            
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    count, error = future.result()
                    total_stored += count
                    tx_pbar.update(count)
                    
                    if error:
                        tx_pbar.write(f"   ❌ {error}")
                        
                except Exception as e:
                    tx_pbar.write(f"   ❌ Unexpected error at index {idx}: {e}")
                

        tx_pbar.close()
        return True



def main():
    print("🚀 Starting Modular Parallel Ingestion...\n")
    
    # Fetch latest blocks from Blockstream

    with tracer.start_as_current_span(
        "bitcoin.api.latest_blocks_fetch",
        attributes={
            "http.method": "GET",
            "http.url": "https://blockstream.info/api/blocks",
        }
    ):
        blocks = get_api_data("https://blockstream.info/api/blocks")
    
    if blocks:
        # Filter to only process the last block (or change this to process more)
        blocks_to_process = blocks[0:2]
        total_blocks = len(blocks_to_process)
        
        print(f"📊 Found {total_blocks} block(s) to index\n")
        
        # Create progress bar for overall block processing
        block_pbar = tqdm(
            total=total_blocks,
            desc="Overall Progress",
            unit="block",
            position=0,
            leave=False,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} blocks [{elapsed}<{remaining}]'
        )
        
        for block in blocks_to_process:
            block_pbar.set_description(f"Processing Block #{block['height']}")
            sync_full_block(block, block_pbar, max_workers=10, transaction_ratio_to_fetch=10)
            block_pbar.update(1)
            # Short rest between blocks (removed for performance)
        
        block_pbar.close()
        print("\n🎉 ALL DONE: Your relational database is fully synced.")


if __name__ == "__main__":
    main()
