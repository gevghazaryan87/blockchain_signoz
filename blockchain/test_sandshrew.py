import sys
import os

# Ensure we can import from the extraction module
sys.path.append(os.getcwd())

from extraction.sandshrew import SandshrewProvider

def test_sandshrew():
    print("🚀 Initializing Sandshrew Provider...")
    provider = SandshrewProvider()
    print(f"Provider Name: {provider.name}")
    print(f"Rate Limit: {provider.rate_limit}")

    print("\n📦 Fetching Latest Blocks...")
    blocks = provider.get_latest_blocks(count=2)
    
    if not blocks:
        print("❌ Failed to fetch blocks.")
        return

    print(f"✅ Fetched {len(blocks)} blocks.")
    for block in blocks:
        print(f"   - Height: {block.get('height')}, Hash: {block.get('id')}, Tx Count: {block.get('tx_count')}")

    if blocks:
        latest_block = blocks[0]
        block_hash = latest_block['id']
        print(f"\n🔍 Fetching transactions for block {block_hash}...")
        
        txs = provider.get_block_transactions(block_hash, start_index=0)
        
        if txs:
            print(f"✅ Fetched {len(txs)} transactions.")
            print(f"   - First Tx: {txs[0].get('txid')}")
        else:
            print("❌ Failed to fetch transactions or block is empty.")

if __name__ == "__main__":
    test_sandshrew()
