import os
import sqlite3
import pandas as pd

CSV_PATH = os.path.join("data", "raw", "paysim.csv")
DB_PATH = os.path.join("outputs", "triage.db")
CHUNK_SIZE = 200000


def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Missing {CSV_PATH}. Put PaySim CSV there.")

    os.makedirs("outputs", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS transactions;")
    conn.commit()

    total = 0
    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        # force lowercase column names and strip spaces
        chunk.columns = [c.strip().lower() for c in chunk.columns]

        # write to sqlite
        chunk.to_sql("transactions", conn, if_exists="append", index=False)

        total += len(chunk)
        print(f"loaded {total:,} rows...")

    # makes later queries faster
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_step ON transactions(step);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_nameorig_step ON transactions(nameorig, step);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(type);")
    conn.commit()

    print(f"sqlite db created at: {DB_PATH}")
    print(f"total rows loaded: {total:,}")

    conn.close()


if __name__ == "__main__":
    main()
