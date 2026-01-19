# AML Alert Triage (PaySim) — Signals → Cases → Daily Queue

## What this is
A lightweight fraud/financial-crime triage pipeline built on PaySim:
- Generate signals from raw transactions (rule-based)
- Aggregate into cases (per account + day)
- Produce a daily review queue (top-K per day)
- Provide investigator drill-down (case → evidence transactions)

## Dataset
- PaySim transaction simulator CSV (not committed)
- Labels (`isFraud`) are **not used to create rules**; they're only for later evaluation (optional)

## How to run
> Assumes `data/raw/paysim.csv` exists.

### 1) Load CSV into SQLite
```bash
python3 src/01_load_paysim_to_sqlite.py
```

### 2) Rule 1 (template): High-velocity outgoing

Note: PaySim constraint — max outgoing tx per account in 4h/24h bucket is 2, so this template rule does not fire on PaySim.
```bash
python3 src/02_rule1_velocity_out.py
```

### 3) Rule 2: Balance anomaly signals → cases
```bash
python3 src/03_rule2_balance_anomaly.py
```

### 4) Build daily queue (top 200/day)
```bash
python3 src/04_make_queue_balance_anomaly.py
```

### 6) Investigator drill-down (pick a case)
```bash
python3 src/05_investigate_case.py
```

### 8) Enrich queue with evidence + reason text
```bash
python3 src/06_enrich_queue_simple.py
```

## Outputs (SQLite tables)
- transactions
- signals_balance_anomaly
- alerts_balance_anomaly
- queue_balance_anomaly
- queue_balance_anomaly_enriched

## Example investigation (1 case)

Queued case:

subject_entity_id: C1715283297

day_start_step: 264

Evidence:

step 276 TRANSFER amount 92445516.64 oldbalanceorg=0 → reason: impossible old balance

## Limitations / next steps
- Add a second rule (counterparty novelty) and a master queue combining multiple signals.
- Add evaluation using isFraud only for measurement (hit-rate @ top-K).
