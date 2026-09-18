# Boulevard ML Demand Model - dbt Project

End-to-end ML demand forecasting pipeline on Snowflake, orchestrated with dbt. Predicts `PROVIDER_HOURS_DEMANDED` per (location x service category x date) using XGBoost with conformal prediction intervals.

## Pipeline DAG

```
M1_1_RAW_TRAINING_DATA           (table)           Feature engineering from SCHED.PUBLISHED.*
  |
  +-- M1_2_LOCATION_SERVICE_MODEL (ml_model)        XGBoost training via @remote ML Job
  |     |                                            Registers model in Snowflake Model Registry
  |     +-- Deploys daily retrain Task DAG           (Cron: 0 6 * * * UTC)
  |
  +-- M1_4_LOCATION_SERVICE_PREDICTION (incremental) Batch inference via MODEL()!PREDICT()
        |                                             Scores new dates incrementally
        +-- M1_6_SEMANTIC_VIEW        (semantic_view) Cortex Analyst interface
        |     |
        |     +-- M1_5_AGENT          (agent)         Cortex Agent for NL queries
        |
        +-- M1_3_MODEL_MONITOR        (model_monitor) Regression drift + performance tracking
```

## Models

| Model | Materialization | Description |
|-------|-----------------|-------------|
| `M1_1_RAW_TRAINING_DATA` | table | Feature engineering: calendar, location, demand lags, marketing, CRM features |
| `M1_2_LOCATION_SERVICE_MODEL` | ml_model | Submits XGBoost training to compute pool, registers ConformalXGBModel in Model Registry |
| `M1_4_LOCATION_SERVICE_PREDICTION` | incremental | Scores data using `MODEL()!PREDICT()`, flattens prediction + confidence intervals |
| `M1_6_SEMANTIC_VIEW` | semantic_view | Exposes predictions for Cortex Analyst natural language queries |
| `M1_5_AGENT` | agent | Cortex Agent wrapping the semantic view for end-user access |
| `M1_3_MODEL_MONITOR` | model_monitor | Tracks RMSE, MAE, drift (PSI) on the prediction table |

## Custom Materializations

| Materialization | File | DDL Generated |
|-----------------|------|---------------|
| `ml_model` | `macros/materializations/ml_model.sql` | Replicates Snowflake table materialization (SQL + Python) |
| `agent` | `macros/materializations/agent.sql` | `CREATE OR REPLACE AGENT <name>` |
| `model_monitor` | `macros/materializations/model_monitor.sql` | `CREATE MODEL MONITOR IF NOT EXISTS <name> WITH` |
| `semantic_view` | via `dbt_semantic_view` package | `CREATE OR REPLACE SEMANTIC VIEW <name>` |

## Setup

### 1. Create and activate the virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dbt and the Snowflake adapter

```bash
python -m pip install --pre dbt
pip install dbt-snowflake
dbt --version

#Initialize dbt :
dbt init 
#change configuraiton file:
dbt run --profiles-dir /Users/lvillavicencio/Documents/Github/dbtdemo_boulevard

```

### 3. Verify connection

```bash
dbt debug
```

### 4. Install dbt packages

add dbt_semantic_view package (packages file) :

packages:
  - package: Snowflake-Labs/dbt_semantic_view
    version: [">=1.0.0"]
```bash
dbt deps
```

## Running the Pipeline

### Full pipeline

```bash
dbt run --select blvd_ai.ml_demand_model
dbt run --select blvd_ai.ml_demand_model --target dev
dbt run --select blvd_ai.ml_demand_model --target prod
```

### Individual models

```bash
dbt run --select M1_1_RAW_TRAINING_DATA
dbt run --select M1_2_LOCATION_SERVICE_MODEL
dbt run --select M1_4_LOCATION_SERVICE_PREDICTION
dbt run --full-refresh --select M1_4_LOCATION_SERVICE_PREDICTION   # rebuild incremental from scratch
dbt run --select M1_6_SEMANTIC_VIEW
dbt run --select M1_5_AGENT
dbt run --select M1_3_MODEL_MONITOR
```

### Tests and lineage

```bash
dbt test
dbt docs generate 
dbt docs serve
```

## Key Technical Details

- **ML Job execution**: M1_2 uses `@remote` decorator to submit training to compute pool `ML_TRAINING_MEM_X64_G2_8`. The `train()` function runs on SPCS, not the dbt warehouse.
- **Automatic retraining**: M1_2 deploys a Snowflake Task DAG (`M1_2_RETRAIN_DAG`) that retrains daily at 6 AM UTC.
- **Incremental inference**: M1_4 only scores dates after the latest already-predicted date. First run scores the last 30 days.
- **cloudpickle workaround**: `ConformalXGBModel.__module__` is patched to `'__main__'` before `reg.log_model()` to ensure the serialized model artifact works at both compute pool and warehouse inference time.

## Versions

| Component | Version |
|-----------|---------|
| dbt-core | 1.12.4 |
| dbt-snowflake | 1.12.0 |
| dbt_semantic_view | 1.0.6 |

## References

- [dbt installation](https://docs.getdbt.com/docs/local/install-dbt?install-method=pip)
- [dbt_semantic_view package](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view)
- [Snowflake ML Jobs](https://docs.snowflake.com/en/developer-guide/snowflake-ml/ml-jobs/overview)
- [Snowflake Model Registry](https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/overview)
- [Snowflake Model Monitor](https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/model-monitor)
