import time
import hashlib
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from config import DB_CONFIG

def get_db_connection(retries=5, delay=2):
    """Attempt to connect to the database with retries."""
    for i in range(retries):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as e:
            if i < retries - 1:
                print(f"⚠️ Database connection failed. Retrying in {delay} seconds... ({i+1}/{retries})")
                time.sleep(delay)
            else:
                print(f"❌ Could not connect to the database after {retries} attempts.")
                raise e

def is_block_fully_synced(block_hash, total_txs):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM bitcoin_transactions WHERE block_hash = %s", (block_hash,))
        count = cur.fetchone()[0]
        return count >= total_txs
    finally:
        cur.close()
        conn.close()

def insert_block_header(block):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bitcoin_blocks (
                block_hash, previous_block_hash, height, version, 
                merkle_root, timestamp, bits, nonce
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (block_hash) DO NOTHING
        """, (
            block['id'], block.get('previousblockhash'), block['height'], 
            block.get('version'), block.get('merkle_root'),
            block['timestamp'], block.get('bits'), block.get('nonce')
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_transaction_batch(transactions, block_hash, base_index=0):
    if not transactions:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for i, tx in enumerate(transactions):
            # Calculate absolute index in the block
            tx_index = base_index + i
            
            # Determine if it's a coinbase transaction
            is_coinbase = any(vin.get('is_coinbase', False) for vin in tx.get('vin', []))
            
            status = tx.get('status', {})

            cur.execute("""
                INSERT INTO bitcoin_transactions (
                    txid, block_hash, block_height, tx_index, version, locktime, is_coinbase
                ) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (txid) DO NOTHING
            """, (
                tx['txid'], block_hash, status.get('block_height'), tx_index,
                tx.get('version'), tx.get('locktime'), is_coinbase
            ))


            # 2. Store Outputs
            for n, vout in enumerate(tx.get('vout', [])):
                cur.execute("""
                    INSERT INTO bitcoin_outputs (
                        txid, output_index, value, script_pubkey, script_pubkey_asm,
                        script_pubkey_type, address
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    tx['txid'], n, vout.get('value'), 
                    vout.get('scriptpubkey'), vout.get('scriptpubkey_asm'),
                    vout.get('scriptpubkey_type'), vout.get('scriptpubkey_address')
                ))


            # 3. Store Inputs (Level 3)
            for n, vin in enumerate(tx.get('vin', [])):
                cur.execute("""
                    INSERT INTO bitcoin_inputs (
                        txid, input_index, prev_txid, prev_vout, 
                        script_sig, script_sig_asm, sequence, is_coinbase
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    tx['txid'], n, 
                    vin.get('txid'), vin.get('vout'),
                    vin.get('scriptsig'), vin.get('scriptsig_asm'),
                    vin.get('sequence'), vin.get('is_coinbase', False)
                ))

                witnesses = vin.get('witness', [])
                
                for w_index, w_data in enumerate(witnesses):
                    cur.execute("""
                        INSERT INTO bitcoin_witnesses (
                            txid, input_index, witness_index, witness
                        ) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """, (
                        tx['txid'], n, w_index, w_data
                    ))  

        conn.commit()


        return len(transactions)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
