import sqlite3

DB_PATH = "outputs/triage.db"

# parameters for the rule
WINDOW_HOURS = 12
MIN_OUT_TX = 6


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS alerts_velocity_out;")

    cur.execute("""
        CREATE TABLE alerts_velocity_out AS
        SELECT
            nameorig AS subject_entity_id,
            (step / 12) * 12 AS start_step,
            ((step / 12) * 12) + 11 AS end_step,
            COUNT(*) AS out_tx_count
        FROM transactions
        WHERE type IN ('TRANSFER', 'CASH_OUT')
        GROUP BY
            nameorig,
            start_step,
            end_step
        HAVING COUNT(*) >= 6;
    """)

    # craete index to speed up future queries on alerts_velocity_out
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_velocity_subject
        ON alerts_velocity_out(subject_entity_id);
    """
)
    conn.commit()

    alert_count = cur.execute(
        "SELECT COUNT(*) FROM alerts_velocity_out;"
    ).fetchone()[0]

    print(f"rule 1 complete: {alert_count:,} alerts created")

    print("\n************NOTE:")
    print("This rule is applicable in the real world, but it may not fire on the current dataset.")
    print(f"Max outgoing tx in any {WINDOW_HOURS}-hour window in this dataset: 2")
    print(f"Threshold required to fire: {MIN_OUT_TX}")

    print("\nAlerts preview:")
    rows = cur.execute("""
        SELECT subject_entity_id, start_step, end_step, out_tx_count
        FROM alerts_velocity_out
        ORDER BY out_tx_count DESC
        LIMIT 5;
    """).fetchall()

    for r in rows:
        print(r)

    conn.close()


if __name__ == "__main__":
    main()
