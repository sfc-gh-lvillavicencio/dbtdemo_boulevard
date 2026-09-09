# Plan: Convert ML Training Script to dbt Python Model

## Context

The file `models/blvd_ai/ml_demand_model/M1_LOCATION_SERVICE_TRAINING_S.py` is currently a native Python script designed to run on a Snowflake compute pool via `snowflake.ml.jobs.remote`. It needs to be converted to a **dbt Python model** that:

1. Trains the XGBoost model with conformal prediction intervals
2. Registers the model in the **Snowflake Model Registry** (primary goal)
3. Materializes an audit/metrics table as the dbt output

## Key Design Decisions

### dbt Python model contract
A dbt Python model must define `def model(dbt, session)` and return a Snowpark DataFrame or pandas DataFrame. That return value becomes the materialized table. Since the primary goal is model registration, we'll:
- Keep all training + registration logic inside `model()`
- Return a single-row metrics DataFrame as the audit table (version, RMSE, MAE, WAPE, MAPE, conformal_q, train/test dates, timestamp)
- Write feature importance to its own table as a side-effect via `session.write_pandas()` / `save_as_table()`

### Package dependencies
dbt Python models on Snowflake run inside a Snowpark stored procedure. The packages `xgboost`, `snowflake-ml-python`, `scikit-learn`, `numpy`, and `pandas` must be available. These are specified via `dbt_project.yml` or a model-level `config()` block using the `packages` key.

### What changes

| Aspect | Before (ML Job) | After (dbt Python model) |
|---|---|---|
| Entry point | `@remote` decorator + `if __name__` | `def model(dbt, session)` |
| Session | `get_active_session()` | Passed as `session` parameter |
| Config (DB/schema) | Hardcoded `USE ROLE/DB/SCHEMA` | dbt `dbt_project.yml` config + `dbt.config()` |
| Compute | Snowflake compute pool | Snowpark warehouse (set in dbt profile) |
| Output | Side-effects only (registry + table) | Returns DataFrame → materialized table |
| Packages | Pre-installed in container | Declared in `dbt.config(packages=...)` |

### What stays the same
- All ML logic: data loading, encoding, training, CV, conformal calibration, feature importance
- Model Registry registration via `snowflake.ml.registry.Registry`
- Feature importance side-effect write

## Implementation Steps

### 1. Rewrite `M1_LOCATION_SERVICE_TRAINING_S.py`

Convert to dbt Python model format:

```python
def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["xgboost", "snowflake-ml-python", "scikit-learn", "numpy", "pandas"],
    )
    
    # ... all training logic from the original script ...
    # Use `session` directly instead of get_active_session()
    # Remove @remote decorator, __main__ block, and job polling
    # Return metrics DataFrame instead of dict
    
    return session.create_dataframe([metrics_row])
```

Key structural changes:
- Remove `@remote` decorator and `if __name__` block
- Remove `get_active_session()` — use the `session` parameter
- Remove `USE ROLE` / `USE DATABASE` / `USE SCHEMA` — dbt handles this
- Move all constants inside `model()` or keep at module level
- Return a pandas/Snowpark DataFrame with one row of metrics
- Keep `ConformalXGBModel` class definition inside `model()` (Snowpark serializes the function)

### 2. Update `dbt_project.yml`

The existing config block already handles database/schema routing:
```yaml
models:
  dbtdemo_boulevard:
    blvd_ai:
      ml_demand_model:
        +database: BLVD_AI
        +schema: ML_DEMAND_MODEL
        +materialized: table
```

No changes needed here — the Python model inherits this config.

### 3. Update `schema.yml`

Document the output columns of the metrics audit table:
- MODEL_NAME, MODEL_VERSION, RMSE, MAE, WAPE_PCT, MAPE_PCT, CV_RMSE, CV_MAE, CV_WAPE_PCT, CV_MAPE_PCT, CONFORMAL_Q, CONFORMAL_COVERAGE_PCT, TRAIN_START, TRAIN_END, TEST_START, TEST_END, TRAINED_AT

### 4. Validate

Run `dbt compile --select M1_LOCATION_SERVICE_TRAINING_S` to verify the model parses correctly.

## Risks / Caveats

- **Warehouse sizing**: The original runs on a dedicated compute pool (`ML_TRAINING_MEM_X64_G2_8`). The dbt model runs on the Snowpark warehouse configured in your dbt profile. Ensure it's large enough for training.
- **Execution time**: dbt models have a default timeout. XGBoost training may need a longer timeout configured in the profile.
- **Side effects**: The feature importance `save_as_table` and model registry `log_model` are side effects — if dbt retries the model, they'll execute again. The `next_version()` helper handles idempotent versioning.
