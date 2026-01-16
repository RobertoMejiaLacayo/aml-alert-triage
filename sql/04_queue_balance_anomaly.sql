-- Build a reviewable daily queue from alerts_balance_anomaly
-- Goal: top 200 cases per day (by severity proxy)

DROP TABLE IF EXISTS queue_balance_anomaly;

CREATE TABLE queue_balance_anomaly AS
SELECT *
FROM (
  SELECT
    a.subject_entity_id,
    CAST(a.start_step AS INTEGER) AS day_start_step,
    CAST(a.end_step   AS INTEGER) AS day_end_step,
    a.signal_count,
    a.total_amount,
    a.impossible_count,
    a.drain_count,

    -- prioritize impossible signals first, then bigger total amounts, then more signals
    (CASE WHEN a.impossible_count >= 1 THEN 1000000000 ELSE 0 END)
      + a.total_amount
      + (a.signal_count * 1000) AS priority_score,

    ROW_NUMBER() OVER (
      PARTITION BY CAST(a.start_step AS INTEGER)
      ORDER BY
        (a.impossible_count >= 1) DESC,
        a.total_amount DESC,
        a.signal_count DESC
    ) AS day_rank

  FROM alerts_balance_anomaly a
)
WHERE day_rank <= 200;

CREATE INDEX IF NOT EXISTS idx_queue_dayrank
  ON queue_balance_anomaly(day_start_step, day_rank);

CREATE INDEX IF NOT EXISTS idx_queue_subject
  ON queue_balance_anomaly(subject_entity_id);
