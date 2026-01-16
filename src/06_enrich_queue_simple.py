import sqlite3
from pathlib import Path

DB_PATH = "outputs/triage.db"
SQL_PATH = "sql/05_enrich_queue_simple.sql"

def main():
    sql = Path(SQL_PATH).read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Execute DROP separately
    cur.execute("DROP TABLE IF EXISTS queue_balance_anomaly_enriched;")
    
    # Rewrite the query to work with SQLite - CREATE TABLE AS with inline CTEs
    create_stmt = """
CREATE TABLE queue_balance_anomaly_enriched AS
WITH max_amount AS (
  SELECT
    q.subject_entity_id,
    q.day_start_step,
    MAX(s.amount) AS max_amount
  FROM queue_balance_anomaly q
  JOIN signals_balance_anomaly s
    ON s.subject_entity_id = q.subject_entity_id
   AND s.step BETWEEN q.day_start_step AND q.day_end_step
  GROUP BY q.subject_entity_id, q.day_start_step
),
chosen_tx AS (
  SELECT
    q.subject_entity_id,
    q.day_start_step,
    MIN(s.tx_rowid) AS chosen_tx_rowid
  FROM queue_balance_anomaly q
  JOIN signals_balance_anomaly s
    ON s.subject_entity_id = q.subject_entity_id
   AND s.step BETWEEN q.day_start_step AND q.day_end_step
  JOIN max_amount m
    ON m.subject_entity_id = q.subject_entity_id
   AND m.day_start_step = q.day_start_step
   AND s.amount = m.max_amount
  GROUP BY q.subject_entity_id, q.day_start_step
)
SELECT
  q.subject_entity_id,
  q.day_start_step,
  q.day_end_step,
  q.day_rank,
  q.priority_score,
  q.signal_count,
  q.total_amount,
  q.impossible_count,
  q.drain_count,
  s.step        AS evidence_step,
  s.type        AS evidence_type,
  s.amount      AS evidence_amount,
  s.namedest    AS evidence_namedest,
  s.reason_code AS evidence_reason_code,
  s.drain_ratio AS evidence_drain_ratio,
  CASE
    WHEN q.impossible_count >= 1 THEN
      'Outgoing TRANSFER/CASH_OUT when old balance was 0 (balance inconsistency)'
    WHEN q.drain_count >= 2 THEN
      'Multiple outgoing transactions drained most of the available balance'
    ELSE
      'Balance anomaly'
  END AS primary_reason_text
FROM queue_balance_anomaly q
JOIN chosen_tx c
  ON c.subject_entity_id = q.subject_entity_id
 AND c.day_start_step = q.day_start_step
JOIN signals_balance_anomaly s
  ON s.tx_rowid = c.chosen_tx_rowid;
"""
    
    cur.execute(create_stmt)
    conn.commit()

    n = cur.execute("SELECT COUNT(*) FROM queue_balance_anomaly_enriched;").fetchone()[0]
    print(f"Simple enriched queue created: {n:,} rows")

    sample = cur.execute("""
        SELECT subject_entity_id, day_start_step, day_rank,
               evidence_step, evidence_type, evidence_amount, evidence_namedest,
               primary_reason_text
        FROM queue_balance_anomaly_enriched
        ORDER BY day_start_step DESC, day_rank ASC
        LIMIT 5;
    """).fetchall()

    print("\nSample enriched rows:")
    for r in sample:
        print(r)

    #check if we accidentally created duplicates
    dup = cur.execute("""
        SELECT COUNT(*) FROM (
          SELECT subject_entity_id, day_start_step, COUNT(*) AS c
          FROM queue_balance_anomaly_enriched
          GROUP BY subject_entity_id, day_start_step
          HAVING c > 1
        );
    """).fetchone()[0]

    print(f"\nDuplicate case-days (from max-amount ties): {dup}")

    conn.close()

if __name__ == "__main__":
    main()
