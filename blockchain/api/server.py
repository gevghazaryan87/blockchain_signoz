"""REST API server for querying indexed blockchain data."""
import os
from flask import Flask, jsonify, request, abort
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG

app = Flask(__name__)


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------


@app.route("/api/blocks")
def get_blocks():
    """Return the latest N blocks.

    Query params:
        count (int): number of blocks to return (default 10, max 100)
    """
    count = min(int(request.args.get("count", 10)), 100)
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM bitcoin_blocks ORDER BY height DESC LIMIT %s;",
            (count,),
        )
        blocks = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(blocks)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/block/<block_hash>")
def get_block(block_hash):
    """Return a single block with its transactions."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            "SELECT * FROM bitcoin_blocks WHERE block_hash = %s;", (block_hash,)
        )
        block = cur.fetchone()
        if not block:
            cur.close()
            conn.close()
            return jsonify({"error": "Block not found"}), 404

        cur.execute(
            "SELECT * FROM bitcoin_transactions WHERE block_hash = %s ORDER BY tx_index ASC;",
            (block_hash,),
        )
        transactions = cur.fetchall()
        cur.close()
        conn.close()

        block["transactions"] = transactions
        return jsonify(block)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


@app.route("/api/tx/<txid>")
def get_transaction(txid):
    """Return full transaction details including inputs, outputs, and witnesses."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Transaction header
        cur.execute("SELECT * FROM bitcoin_transactions WHERE txid = %s;", (txid,))
        tx = cur.fetchone()
        if not tx:
            cur.close()
            conn.close()
            return jsonify({"error": "Transaction not found"}), 404

        # 2. Outputs
        cur.execute(
            "SELECT * FROM bitcoin_outputs WHERE txid = %s ORDER BY output_index;",
            (txid,),
        )
        tx["vout"] = cur.fetchall()

        # 3. Inputs
        cur.execute(
            "SELECT * FROM bitcoin_inputs WHERE txid = %s ORDER BY input_index;",
            (txid,),
        )
        tx["vin"] = cur.fetchall()

        # 4. Witnesses
        cur.execute(
            """
            SELECT input_index, witness_index, witness
            FROM bitcoin_witnesses
            WHERE txid = %s
            ORDER BY input_index, witness_index
        """,
            (txid,),
        )
        witness_rows = cur.fetchall()

        witnesses = {}
        for row in witness_rows:
            idx = row["input_index"]
            if idx not in witnesses:
                witnesses[idx] = []
            witnesses[idx].append(row["witness"])
        tx["witnesses"] = witnesses

        cur.close()
        conn.close()
        return jsonify(tx)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@app.route("/api/stats")
def get_stats():
    """Return block statistics from the aggregated view."""
    count = min(int(request.args.get("count", 10)), 100)
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM block_stats_view ORDER BY height DESC LIMIT %s;",
            (count,),
        )
        stats = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=False)
