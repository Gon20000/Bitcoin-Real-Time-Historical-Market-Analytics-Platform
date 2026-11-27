"""Register Silver Delta table in Trino Catalog."""

from __future__ import annotations

import os
import sys

try:
    from trino import dbapi

    conn = dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user="admin",
        catalog="delta",
        schema="default",
    )
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS delta.silver")
    cur.execute("CREATE SCHEMA IF NOT EXISTS delta.gold")
    cur.execute("SHOW TABLES FROM delta.silver LIKE 'binance_klines_1s'")
    if not cur.fetchall():
        cur.execute(
            "CALL delta.system.register_table("
            "schema_name => 'silver', "
            "table_name => 'binance_klines_1s', "
            "table_location => '/opt/spark/data/silver/stg_binance_klines_1s')"
        )
        print("Registered delta.silver.binance_klines_1s in Trino catalog.")
    else:
        print("Table delta.silver.binance_klines_1s is already registered in Trino.")
    cur.close()
    conn.close()
except Exception as e:  # noqa: BLE001
    print(f"Error registering silver delta table in Trino: {e}", file=sys.stderr)
    sys.exit(1)
