DROP TABLE IF EXISTS queue_balance_anomaly_enriched;

-- find max evidence amount per case-day
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

-- tiebreak: among rows at max_amount, pick only one tx_rowid (smallest)
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

-- build enriched queue by joining the chosen evidence row
CREATE TABLE queue_balance_anomaly_enriched AS
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
