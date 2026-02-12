import psycopg2

from config import DB_CONFIG

def setup_database():
    """Build the full relational schema: Blocks -> Transactions -> (Vins & Vouts)."""
    print("Rebuilding database schema with Vin/Vout support...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # We start by dropping in reverse order of dependencies
        print("Cleaning up old tables...")
        cur.execute("DROP TABLE IF EXISTS bitcoin_witnesses CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_inputs CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_outputs CASCADE;")

        cur.execute("DROP TABLE IF EXISTS bitcoin_transactions CASCADE;")

        cur.execute("DROP TABLE IF EXISTS bitcoin_blocks CASCADE;")
        
        # 1. Blocks Table (Level 1)
        print("Creating table: bitcoin_blocks...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_blocks (
                block_hash VARCHAR(64) PRIMARY KEY,
                previous_block_hash VARCHAR(64),
                height INTEGER UNIQUE NOT NULL,
                version BIGINT,
                merkle_root VARCHAR(64),
                timestamp BIGINT NOT NULL,
                bits VARCHAR(64),
                nonce BIGINT
            );
        """)
        print("✅ Table bitcoin_blocks created or exists.")
        
        # 2. Transactions Table (Level 2)
        print("Creating table: bitcoin_transactions...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_transactions (
                txid VARCHAR(64) PRIMARY KEY,
                block_hash VARCHAR(64) REFERENCES bitcoin_blocks(block_hash) ON DELETE CASCADE,
                block_height INTEGER,
                tx_index INTEGER,
                version INTEGER,
                locktime BIGINT,
                is_coinbase BOOLEAN
            );
        """)
        print("✅ Table bitcoin_transactions created or exists.")



        # 3. Outputs Table (Detail of Transaction - "Money Created")
        print("Creating table: bitcoin_outputs...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_outputs (
                txid VARCHAR(64) REFERENCES bitcoin_transactions(txid) ON DELETE CASCADE,
                output_index INTEGER,
                value BIGINT,
                script_pubkey TEXT,
                script_pubkey_asm TEXT,
                script_pubkey_type VARCHAR(50),
                address VARCHAR(100),
                PRIMARY KEY (txid, output_index)
            );
        """)
        print("✅ Table bitcoin_outputs created or exists.")


        # 4. Inputs Table (Detail of Transaction - "Money Spent")
        print("Creating table: bitcoin_inputs...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_inputs (
                txid VARCHAR(64) REFERENCES bitcoin_transactions(txid) ON DELETE CASCADE,
                input_index INTEGER,
                prev_txid VARCHAR(64),
                prev_vout BIGINT,
                script_sig TEXT,
                script_sig_asm TEXT,
                sequence BIGINT,
                is_coinbase BOOLEAN,
                PRIMARY KEY (txid, input_index)
            );
        """)
        print("✅ Table bitcoin_inputs created or exists.")

        # 6. Aggregated View
        print("Creating view: block_stats_view...")
        cur.execute("""
            CREATE OR REPLACE VIEW block_stats_view AS
            SELECT 
                b.height,
                b.block_hash,
                b.timestamp,
                COUNT(DISTINCT t.txid) AS transaction_count,
                COALESCE(SUM(o.value), 0) AS total_volume_sats,
                (COALESCE(SUM(o.value), 0) / 100000000.0) AS total_volume_btc
            FROM 
                bitcoin_blocks b
            LEFT JOIN 
                bitcoin_transactions t ON b.block_hash = t.block_hash
            LEFT JOIN 
                bitcoin_outputs o ON t.txid = o.txid
            GROUP BY 
                b.height, b.block_hash, b.timestamp;
        """)
        print("✅ View block_stats_view created or updated.")

        print("Creating witness table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_witnesses (
                txid VARCHAR(64) NOT NULL,
                input_index INTEGER NOT NULL,
                witness_index INTEGER NOT NULL,
                witness TEXT NOT NULL,
                PRIMARY KEY (txid, input_index, witness_index),
                FOREIGN KEY (txid, input_index) 
                    REFERENCES bitcoin_inputs(txid, input_index) 
                    ON DELETE CASCADE
            );
        """)
        print("✅ Table bitcoin_witnesses created or exists.    ")


        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Full Relational Blockchain Schema is ready!")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    setup_database()
