import sqlite3
from pathlib import Path

DB_PATH = "outputs/triage.db"
SQL_PATH = "sql/04_queue_balance_anomaly.sql"

def main():
    sql = Path(SQL_PATH).read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(sql)
    conn.commit()

    total = cur.execute("SELECT COUNT(*) FROM queue_balance_anomaly;").fetchone()[0]
    days = cur.execute("SELECT COUNT(DISTINCT day_start_step) FROM queue_balance_anomaly;").fetchone()[0]

    print(f"queue created: {total:,} rows across {days} days")
    print("Sample (top 5 overall):")
    rows = cur.execute("""
        SELECT subject_entity_id, day_start_step, total_amount, impossible_count, drain_count, priority_score, day_rank
        FROM queue_balance_anomaly
        ORDER BY priority_score DESC
        LIMIT 5;
    """).fetchall()
    for r in rows:
        print(r)

    conn.close()

if __name__ == "__main__":
    main()
