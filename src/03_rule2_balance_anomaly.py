import sqlite3

DB_PATH = "outputs/triage.db"
# parameters for the rule
DRAIN_RATIO = 0.90          # amount >= 90% of old balance
MIN_LARGE_AMOUNT = 200000   # only treat large drains as alert-worthy. otherwise too noisy
DAY_HOURS = 24

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1) transaction amount flags
    cur.execute("DROP TABLE IF EXISTS signals_balance_anomaly;")
    cur.execute(f"""
        CREATE TABLE signals_balance_anomaly AS
        SELECT
            rowid AS tx_rowid,
            step,
            type,
            amount,
            nameorig AS subject_entity_id,
            namedest,
            oldbalanceorg,
            newbalanceorig,
            CASE
              WHEN oldbalanceorg = 0 AND amount > 0 THEN 'IMPOSSIBLE_OLD_BALANCE'
              WHEN oldbalanceorg > 0 AND (amount * 1.0 / oldbalanceorg) >= {DRAIN_RATIO} THEN 'DRAINS_BALANCE'
              ELSE 'OTHER'
            END AS reason_code,
            CASE
              WHEN oldbalanceorg > 0 THEN (amount * 1.0 / oldbalanceorg)
              ELSE NULL
            END AS drain_ratio
        FROM transactions
        WHERE type IN ('TRANSFER','CASH_OUT')
          AND (
            (oldbalanceorg = 0 AND amount > 0)
            OR
            (oldbalanceorg > 0 AND (amount * 1.0 / oldbalanceorg) >= {DRAIN_RATIO})
          );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sig_bal_subject ON signals_balance_anomaly(subject_entity_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sig_bal_step ON signals_balance_anomaly(step);")

    # 2) aggregate to alerts based on a day window
    cur.execute("DROP TABLE IF EXISTS alerts_balance_anomaly;")
    cur.execute(f"""
        CREATE TABLE alerts_balance_anomaly AS
        SELECT
            subject_entity_id,
            (step / {DAY_HOURS}) * {DAY_HOURS} AS start_step,
            ((step / {DAY_HOURS}) * {DAY_HOURS}) + ({DAY_HOURS} - 1) AS end_step,
            COUNT(*) AS signal_count,
            SUM(amount) AS total_amount,
            MAX(drain_ratio) AS max_drain_ratio,
            SUM(CASE WHEN reason_code = 'IMPOSSIBLE_OLD_BALANCE' THEN 1 ELSE 0 END) AS impossible_count,
            SUM(CASE WHEN reason_code = 'DRAINS_BALANCE' THEN 1 ELSE 0 END) AS drain_count
        FROM signals_balance_anomaly
        GROUP BY subject_entity_id, start_step, end_step
        HAVING
        (impossible_count >= 2)
        OR
        (impossible_count >= 1 AND total_amount >= 200000)
        OR
        (drain_count >= 2 AND total_amount >= 200000)
;
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_bal_subject ON alerts_balance_anomaly(subject_entity_id);")
    conn.commit()

    # 3) report counts and samples
    sig_n = cur.execute("SELECT COUNT(*) FROM signals_balance_anomaly;").fetchone()[0]
    alert_n = cur.execute("SELECT COUNT(*) FROM alerts_balance_anomaly;").fetchone()[0]

    print(f"Signals created: {sig_n:,}")
    print(f"Alerts (cases) created: {alert_n:,}")

    breakdown = cur.execute("""
        SELECT reason_code, COUNT(*)
        FROM signals_balance_anomaly
        GROUP BY reason_code
        ORDER BY COUNT(*) DESC;
    """).fetchall()

    print("\nSignal breakdown:")
    for r in breakdown:
        print(r)

    sample = cur.execute("""
        SELECT subject_entity_id, start_step, end_step, signal_count, total_amount, impossible_count, drain_count
        FROM alerts_balance_anomaly
        ORDER BY total_amount DESC
        LIMIT 5;
    """).fetchall()

    print("\nTop 5 alerts (by total_amount):")
    for r in sample:
        print(r)

    conn.close()

if __name__ == "__main__":
    main()
