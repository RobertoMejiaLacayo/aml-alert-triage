# AML Alert Triage (PaySim) — Signals → Cases → Daily Queue

## What this is
A lightweight fraud/financial-crime triage pipeline built on PaySim:
- Generate **signals** from raw transactions (rule-based)
- Aggregate into **cases** (per account + day)
- Produce a **daily review queue** (top-K per day)
- Provide **investigator drill-down** (case → evidence transactions)

## Dataset
- PaySim transaction simulator CSV (not committed)
- Labels (`isFraud`) are **not used to create rules** — only for later evaluation (optional)

## How to run
> Assumes `data/raw/paysim.csv` exists.

### 1) Load CSV into SQLite
```bash
python3 src/01_load_paysim_to_sqlite.py
