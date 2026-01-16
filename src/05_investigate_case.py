import sqlite3

DB_PATH = "outputs/triage.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    subject = input("subject_entity_id (e.g., C123...): ").strip()
    day_start = int(input("day_start_step (e.g., 264): ").strip())
    day_end = day_start + 23

    # 1) get the queued case info
    case = cur.execute("""
        SELECT subject_entity_id, day_start_step, day_end_step,
               signal_count, total_amount, impossible_count, drain_count,
               priority_score, day_rank
        FROM queue_balance_anomaly
        WHERE subject_entity_id = ?
          AND day_start_step = ?;
    """, (subject, day_start)).fetchone()

    if not case:
        print("❌ No queued case found for that subject/day.")
        conn.close()
        return

    print("\n=== QUEUED CASE ===")
    print(case)

    # pull the underlying signal transactions for that subject/day
    print("\n=== EVIDENCE SIGNALS (up to 30) ===")
    signals = cur.execute("""
        SELECT step, type, amount, namedest,
               oldbalanceorg, newbalanceorig,
               reason_code, drain_ratio
        FROM signals_balance_anomaly
        WHERE subject_entity_id = ?
          AND step BETWEEN ? AND ?
        ORDER BY
          CASE reason_code
            WHEN 'IMPOSSIBLE_OLD_BALANCE' THEN 0
            ELSE 1
          END,
          amount DESC
        LIMIT 30;
    """, (subject, day_start, day_end)).fetchall()

    if not signals:
        print("No signals found in that window (unexpected).")
    else:
        for s in signals:
            print(s)

    # pull the raw transactions in that window for context
    print("\n=== RAW TX CONTEXT (top 20 outgoing in window) ===")
    txs = cur.execute("""
        SELECT step, type, amount, nameorig, namedest, oldbalanceorg, newbalanceorig
        FROM transactions
        WHERE nameorig = ?
          AND step BETWEEN ? AND ?
          AND type IN ('TRANSFER','CASH_OUT')
        ORDER BY amount DESC
        LIMIT 20;
    """, (subject, day_start, day_end)).fetchall()

    for t in txs:
        print(t)

    conn.close()

if __name__ == "__main__":
    main()
