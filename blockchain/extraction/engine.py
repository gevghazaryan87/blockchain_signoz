"""Ingestion engine — orchestrates parallel block and transaction syncing."""
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from opentelemetry import trace

from db.operations import insert_block_header, insert_transaction_batch, is_block_fully_synced
from extraction.base import BlockchainProvider
from extraction.pool import ProviderPool

tracer = trace.get_tracer("bitcoin_ingest")


def fetch_and_store_batch(pool: ProviderPool, block: dict, idx: int, total_txs: int):
    """Fetch and store a single batch of transactions using the provider pool."""
    block_hash = block["id"]
    
    # Get the next provider from the pool
    provider = pool.get_next_provider()

    with tracer.start_as_current_span(
        "bitcoin.block.txs.batch",
        attributes={
            "bitcoin.block.height": block["height"],
            "bitcoin.block.hash": block_hash,
            "batch.start_index": idx,
            "batch.size": 25,
            "block.total_txs": total_txs,
            "provider": provider.name,
        },
    ):
        with tracer.start_as_current_span(
            "bitcoin.api.fetch",
            attributes={
                "http.method": "GET",
                "provider": provider.name,
                "bitcoin.block.hash": block_hash,
                "batch.start_index": idx,
            },
        ):
            tx_data = provider.get_block_transactions(block_hash, idx)

        if not tx_data:
            # Report failure to pool so it can pause this provider
            pool.report_rate_limit(provider.name, retry_after=30)
            return 0, f"Batch at index {idx} failed ({provider.name})"

        try:
            with tracer.start_as_current_span(
                "bitcoin.db.ingest",
                attributes={
                    "bitcoin.block.hash": block_hash,
                    "batch.start_index": idx,
                    "tx.count": len(tx_data),
                },
            ):
                count = insert_transaction_batch(tx_data, block_hash, base_index=idx)

            return count, None
        except Exception as e:
            return 0, f"Batch store failed at index {idx}: {e}"


def sync_full_block(
    pool: ProviderPool,
    block: dict,
    block_pbar=None,
    max_workers: int = 5,
    transaction_ratio_to_fetch: int = 100,
):
    """Orchestrates parallel fetching and storage of all transactions in a block."""
    block_hash = block["id"]
    total_txs = block["tx_count"]
    txs_to_fetch = int(transaction_ratio_to_fetch * total_txs / 100)

    with tracer.start_as_current_span(
        "bitcoin.block.sync",
        attributes={
            "bitcoin.block.height": block["height"],
            "bitcoin.block.hash": block_hash,
            "bitcoin.block.max_workers": max_workers,
            "fetch_mode": "multi",
        },
    ):
        if is_block_fully_synced(block_hash, total_txs):
            if block_pbar:
                block_pbar.write(
                    f"✅ Block #{block['height']} is already fully indexed. Skipping."
                )
            return True

        # 1. Store Header
        insert_block_header(block)

        # 2. Setup Pagination
        indices = list(range(0, txs_to_fetch, 25))

        tx_pbar = tqdm(
            total=total_txs,
            desc=f"Block #{block['height']}",
            unit="tx",
            leave=True,
            position=1,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(fetch_and_store_batch, pool, block, idx, total_txs): idx
                for idx in indices
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    count, error = future.result()
                    tx_pbar.update(count)

                    if error:
                        tx_pbar.write(f"   ❌ {error}")
                except Exception as e:
                    tx_pbar.write(f"   ❌ Unexpected error at index {idx}: {e}")

        tx_pbar.close()
        return True


def get_provider(name: str = "blockstream") -> BlockchainProvider:
    """Factory: return a provider instance by name."""
    if name == "blockchain_info":
        from extraction.blockchain_info import BlockchainInfoProvider
        return BlockchainInfoProvider()
    elif name == "blockchair":
        from extraction.blockchair import BlockchairProvider
        return BlockchairProvider()
    elif name == "blockstream":
        from extraction.blockstream import BlockstreamProvider
        return BlockstreamProvider()
    elif name == "emzy":
        from extraction.emzy import EmzyProvider
        return EmzyProvider()
    elif name == "mempool":
        from extraction.mempool import MempoolProvider
        return MempoolProvider()
    elif name == "ninja":
        from extraction.ninja import NinjaProvider
        return NinjaProvider()
    elif name == "sandshrew":
        from extraction.sandshrew import SandshrewProvider
        return SandshrewProvider()
    else:
        from extraction.blockstream import BlockstreamProvider
        return BlockstreamProvider()


def get_pool(mode: str) -> ProviderPool:
    """Build a provider pool based on the pool mode."""
    from extraction.blockstream import BlockstreamProvider
    from extraction.mempool import MempoolProvider
    from extraction.emzy import EmzyProvider
    from extraction.blockchair import BlockchairProvider
    from extraction.blockchain_info import BlockchainInfoProvider
    from extraction.sandshrew import SandshrewProvider

    if mode == "multi":
        # In multi mode, use all capable providers
        return ProviderPool([
            BlockstreamProvider(),
            MempoolProvider(),
            EmzyProvider(),
            SandshrewProvider(),
            # BlockchairProvider(),
            BlockchainInfoProvider()
        ])
    else:
        # In single mode, assume PROVIDER env var selects one, wrapped in a pool of 1
        name = os.getenv("PROVIDER", "blockstream")
        return ProviderPool([get_provider(name)])


def main():
    import sys
    
    # Check command line args first, then fall back to environment variable
    if len(sys.argv) > 1 and sys.argv[1] in ["single", "multi"]:
        fetch_mode = sys.argv[1]
    else:
        fetch_mode = os.getenv("FETCH_MODE", "single")
        
    pool = get_pool(fetch_mode)
    
    # Use the first provider in the pool to fetch latest blocks (usually reliable one)
    main_provider = pool.get_all_providers()[0]

    print(f"🚀 Starting Ingestion (Mode: {fetch_mode}, Providers: {[p.name for p in pool.get_all_providers()]})...\n")

    with tracer.start_as_current_span(
        "bitcoin.api.latest_blocks_fetch",
        attributes={"provider": main_provider.name},
    ):
        blocks = main_provider.get_latest_blocks(count=10)

    if blocks:
        blocks_to_process = blocks[:5]
        total_blocks = len(blocks_to_process)

        print(f"📊 Found {total_blocks} block(s) to index\n")

        block_pbar = tqdm(
            total=total_blocks,
            desc="Overall Progress",
            unit="block",
            position=0,
            leave=False,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} blocks [{elapsed}<{remaining}]",
        )

        for block in blocks_to_process:
            block_pbar.set_description(f"Processing Block #{block['height']}")
            sync_full_block(pool, block, block_pbar, max_workers=10, transaction_ratio_to_fetch=10) # High workers for multi-mode pool
            block_pbar.update(1)

        block_pbar.close()
        print("\n🎉 ALL DONE: Your relational database is fully synced.")


if __name__ == "__main__":
    main()
