# Location-Service XGBoost demand model training pipeline (ML Jobs)
#
# production training script that runs on a Snowflake compute pool via
# snowflake.ml.jobs.remote.
#
# Pipeline:
#   1. Load data from LOCATION_SERVICE_TRAINING_RAW with temporal train/test split
#   2. Encode categoricals (pandas CategoricalDtype, XGBoost native handling)
#   3. Train XGBRegressor with TimeSeriesSplit cross-validation
#   4. Calibrate split conformal prediction intervals on the test set
#   5. Register ConformalXGBModel in Snowflake Model Registry with metrics

from snowflake.ml.jobs import remote
from snowflake.ml.jobs import get_job, delete_job
import os

ENV       = os.environ.get("var_environment", "DEV")
DB        = None #"BLVD_AI"
SCHEMA    = None #"ML_DEMAND_MODEL"
FQN       = None #f"{DB}.{SCHEMA}"
TABLE     = None #"LOCATION_SERVICE_TRAINING_RAW"
ROLE = os.environ.get("var_role", "SNOWFLAKE_PS")
TARGET_COL = "PROVIDER_HOURS_DEMANDED"

MODEL_NAME = None #"LOCATION_SERVICE_DEMAND_XGBOOST"

# Training / test date windows
START_TRAIN_DATE  = os.environ.get("var_start_train_date", "2025-01-01")
FINISH_TRAIN_DATE = os.environ.get("var_finish_train_date", "2026-03-31")
START_TEST_DATE   = os.environ.get("var_start_test_date", "2026-04-01")
FINISH_TEST_DATE  = os.environ.get("var_finish_test_date", "2026-04-30")

# Conformal prediction alpha (90% interval)
ALPHA = 0.10


COMPUTE_POOL = os.environ.get("var_compute_pool", "ML_TRAINING_MEM_X64_G2_8")
STAGE_NAME   = "ML_JOB_PAYLOAD_STAGE" #f"@{DB}.{SCHEMA}.{MODEL_NAME}"


# ─────────────────────────────── Feature columns ─────────────────────────────
# Numeric features — passed to XGBoost unchanged.
# Uncomment / comment columns below to experiment with different feature sets.
# Column order mirrors SQL sections in 1_RAW_TRAINING_DATA_CURATED.sql.

NUMERIC_FEATURE_COLS = [
    # ── 3.1 Calendar ──────────────────────────────────────────────────────────
    "DAY_OF_WEEK", "DAY_OF_MONTH",
    #"MONTH_NUM",
    #"QUARTER",
    "WEEK_OF_YEAR", "DAY_OF_YEAR", "IS_WEEKEND", "IS_HOLIDAY",
    "DAYS_TO_NEAREST_HOLIDAY",
    # ── 3.2 Location ──────────────────────────────────────────────────────────
    "LOCATION_AGE_DAYS",
    # "LATITUDE",   # removed: geographic signal covered by LOCATION_STATE/CITY/ZIP3
    # "LONGITUDE",  # removed: same as LATITUDE
    # (LICENSE_TIER, LOCATION_CITY → CATEGORICAL_COLS below;
    #  LOCATION_STATE and LOCATION_ZIP3 removed — see CATEGORICAL_COLS)
    # ── 3.3 Provider / Workforce ──────────────────────────────────────────────
    "LOCATION_STAFF_COUNT",
    #"AVG_PROVIDER_TENURE_DAYS",
    #"NUM_PROVIDERS_SCHEDULED",
    #"TOTAL_AVAILABLE_HOURS",
    #"UTILIZATION_RATE_RECENT",
    # ── 3.4 Service / Menu ────────────────────────────────────────────────────
    #"NUM_SERVICES_OFFERED",
    #"AVG_SERVICE_DURATION_MIN",
    #"AVG_SERVICE_PRICE_CENTS",
    #"NUM_QUALIFIED_PROVIDERS",
    # ── 3.5 Demand lags — appointment counts ──────────────────────────────────
    "COUNT_APPTS_L7D", "COUNT_APPTS_L30D", "COUNT_APPTS_DOW_AVG",
    "COUNT_APPTS_LAG_7D", "COUNT_APPTS_LAG_14D", "COUNT_APPTS_LAG_28D",
    "COUNT_APPTS_LAG_364D", "COUNT_APPTS_TREND_4W",
    # ── 3.5 Demand lags — provider hours ──────────────────────────────────────
    "HOURS_PROVIDER_L7D", "HOURS_PROVIDER_L30D", "HOURS_PROVIDER_DOW_AVG",
    "HOURS_PROVIDER_LAG_7D", "HOURS_PROVIDER_LAG_14D", "HOURS_PROVIDER_LAG_28D",
    "HOURS_PROVIDER_LAG_364D", "HOURS_PROVIDER_TREND_4W", "HOURS_PROVIDER_PER_APPT_L30D",
    # ── 3.5a Competitor Demand — ZIP3 (broader local market) ──────────────────
    # Competitor-only rolling features (market total − self). Available in
    # LOCATION_SERVICE_TRAINING_RAW but not yet wired into 4_DEMAND_FORECAST.py.
    # Uncomment to include in experiments.
    #"COMP_ZIP3_COUNT_APPTS_L7D", "COMP_ZIP3_COUNT_APPTS_L30D",
    #"COMP_ZIP3_COUNT_APPTS_LAG_7D", "COMP_ZIP3_COUNT_APPTS_LAG_28D",
    #"COMP_ZIP3_HOURS_PROVIDER_L7D", "COMP_ZIP3_HOURS_PROVIDER_L30D",
    #"COMP_ZIP3_HOURS_PROVIDER_LAG_7D", "COMP_ZIP3_HOURS_PROVIDER_LAG_28D",
    #"ZIP3_COUNT_APPTS_DOW_AVG",   # market-level DOW avg (incl. self)
    #"ZIP3_HOURS_PROVIDER_DOW_AVG",
    #"ZIP3_NUM_LOCATIONS",          # market concentration (1 = monopoly)
    # ── 3.5b Competitor Demand — Full ZIP (tightest competition cluster) ───────
    #"COMP_ZIP_COUNT_APPTS_L7D", "COMP_ZIP_COUNT_APPTS_L30D",
    #"COMP_ZIP_COUNT_APPTS_LAG_7D", "COMP_ZIP_COUNT_APPTS_LAG_28D",
    #"COMP_ZIP_HOURS_PROVIDER_L7D", "COMP_ZIP_HOURS_PROVIDER_L30D",
    #"COMP_ZIP_HOURS_PROVIDER_LAG_7D", "COMP_ZIP_HOURS_PROVIDER_LAG_28D",
    #"ZIP_COUNT_APPTS_DOW_AVG",    # market-level DOW avg (incl. self)
    #"ZIP_HOURS_PROVIDER_DOW_AVG",
    #"ZIP_NUM_LOCATIONS",           # market concentration (1 = solo in zip)
    # ── 3.6 Behavioural ───────────────────────────────────────────────────────
    "AVG_LEAD_TIME_DAYS",
    #"BOOKED_INTERNALLY_RATIO", "BOOKED_ONLINE_RATIO", "STAFF_REQUESTED_RATIO",
    # ── 3.7 Pricing ───────────────────────────────────────────────────────────
    # "AVG_PRICE_CENTS",   # not yet evaluated vs AVG_SERVICE_PRICE_CENTS (menu)
    #"AVG_DISCOUNT_CENTS", "DISCOUNT_ADOPTION_RATIO",
    # "AVG_DISCOUNT_PCT",  # absolute (AVG_DISCOUNT_CENTS) is active — TBD
    # ── 3.8 Marketing ─────────────────────────────────────────────────────────
    "EMAIL_MSGS_L30D", "SMS_MSGS_L7D",
    #"TOTAL_MSGS_L30D",
    # ── 3.9 CRM / Salesforce ──────────────────────────────────────────────────
    #"SF_ACTIVE_LOCATIONS", "SF_TOTAL_ARR", "SF_AVG_GMV_PER_LOC",
    #"SF_GPV_PER_APPT", "SF_TAKE_RATE", "SF_INITIAL_ACV",
    #"SF_EST_MONTHLY_APPTS", "SF_IS_ENTERPRISE", "SF_IS_FRANCHISE",
    #"SF_IS_CORPORATE", "SF_PAYMENT_PROCESSING",
    "SF_ACCOUNT_TENURE",
]

# Categorical features — encoded using pandas CategoricalDtype.
# XGBoost handles them natively with enable_categorical=True — no one-hot needed.
# Excluded from categoricals: LOCATION_ID and SERVICE_CATEGORY_ID are entity keys
# (kept active here because cardinality is manageable and XGBoost splits on them).
CATEGORICAL_COLS = [
    #"LICENSE_TIER",
    # "LOCATION_ZIP3",
    #"LOCATION_ID",
    "SERVICE_CATEGORY",
    #"LOCATION_STATE",
    "LOCATION_CITY",
    #"SF_VERTICAL",
    "SF_SEGMENT",
    #"SF_PLAN_TYPE",
    "SF_INDUSTRY",
    #"SF_ACCOUNT_TYPE",
]

ALL_COLS = NUMERIC_FEATURE_COLS + CATEGORICAL_COLS + [TARGET_COL]

def model(dbt, session):
    """dbt Python model entry point — submits the ML Job and waits for results."""
    import time as _time
    import pandas as pd
    dbt.config(
        materialized="ml_model",
        packages=["snowflake-ml-python", "snowflake", "xgboost", "scikit-learn", "numpy", "pandas"],
    )
    # Derive DB/SCHEMA/TABLE from dbt.ref() to maintain DAG lineage.
    global DB, SCHEMA, FQN, TABLE, STAGE_NAME, MODEL_NAME
    TABLE  = dbt.ref("M1_1_RAW_TRAINING_DATA").table_name
    DB     = dbt.this.database
    SCHEMA = dbt.this.schema
    MODEL_NAME = dbt.this.identifier
    FQN    = f"{DB}.{SCHEMA}"
    STAGE_NAME = f"@{DB}.{SCHEMA}.{MODEL_NAME}"

    job = train()
    while job.status in ("PENDING", "RUNNING"):
        _time.sleep(10)
    if job.status != "DONE":
        raise RuntimeError(f"ML Job {job.id} failed with status: {job.status}")
    result = job.result()

    # Clean up the job
    #delete_job(job)

    # Deploy a scheduled DAG so the model retrains daily
    deploy_retrain_schedule(session)

    df = pd.DataFrame([result])
    return df


def deploy_retrain_schedule(session):
    """Deploy a Snowflake Task DAG that retrains the model daily at 6 AM UTC.
    Uses the @remote-decorated train() function as the ML Job Definition.
    Runs one immediate execution; the cron schedule handles subsequent runs."""
    from snowflake.core import Root, CreateMode
    from snowflake.core.task import Cron
    from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation

    dag_name = "M1_2_RETRAIN_DAG"
    with DAG(dag_name,
             schedule=Cron("0 6 * * *", "UTC"),
             warehouse="SNOWFLAKE_PS_STANDARD") as dag:
        retrain_task = DAGTask("RETRAIN_MODEL", definition=train)

    schema = Root(session).databases[DB].schemas[SCHEMA]
    op = DAGOperation(schema)
    op.deploy(dag, mode=CreateMode.or_replace)
    #op.run(dag)  # immediate one-off run; schedule handles the rest ( trigered in model() above)


@remote(compute_pool=COMPUTE_POOL, stage_name=STAGE_NAME, target_instances=1)
def train():
    import time
    import os
    import pickle
    import numpy as np
    import pandas as pd
    import xgboost as xgb
    from sklearn.metrics import mean_squared_error, make_scorer
    from sklearn.model_selection import cross_validate, TimeSeriesSplit
    from snowflake.snowpark.context import get_active_session
    from snowflake.ml.registry import Registry
    from snowflake.ml.model import custom_model, task, type_hints

    # ─────────────────────── Custom model (conformal XGBoost) ────────────────
    # Wraps the trained XGBoost model with split conformal prediction (post-hoc)
    # so that the registered model's predict() natively returns point estimates
    # + 90% prediction intervals: [PREDICTED, PRED_LOWER_05, PRED_UPPER_95].
    #
    # Handles string→category conversion and unseen categories for SQL inference.
    # Category vocabulary is loaded from the pickled category_mappings artifact
    # that was fitted on the TRAINING split only (no leakage from test data).
    # Unseen categories become NaN → XGBoost handles them as missing.

    class ConformalXGBModel(custom_model.CustomModel):
        """XGBoost with split conformal prediction intervals."""

        def __init__(self, context: custom_model.ModelContext) -> None:
            super().__init__(context)
            import pickle, xgboost as xgb
            with open(context.path("calibration"), "rb") as f:
                self._cal = pickle.load(f)
            with open(context.path("category_mappings"), "rb") as f:
                self._cat_mappings = pickle.load(f)
            self._model = xgb.XGBRegressor()
            self._model.load_model(context.path("xgb_model"))
            # Identify categorical feature columns from the booster metadata
            self._cat_cols = [
                f for f, t in zip(
                    self._model.get_booster().feature_names,
                    self._model.get_booster().feature_types,
                ) if t == "c"
            ]

        @custom_model.inference_api
        def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
            # Cast string columns to category using training vocabulary.
            # Unseen categories become NaN → XGBoost handles them as missing.
            for col in self._cat_cols:
                if col in input_df.columns:
                    input_df[col] = pd.Categorical(
                        input_df[col].astype(str).values,
                        dtype=self._cat_mappings[col],
                    )
            preds = self._model.predict(input_df)
            q = self._cal["conformal_q"]
            lower = np.maximum(preds - q, 0.0)
            upper = preds + q
            return pd.DataFrame({
                "PREDICTED": preds,
                "PRED_LOWER_05": lower,
                "PRED_UPPER_95": upper,
            })

    # ─────────────────────── Helper: version naming ──────────────────────────

    def next_version(registry, name):
        try:
            model_ref = registry.get_model(name)
            versions = [v.version_name for v in model_ref.versions()]
            nums = [int(v.lstrip("V")) for v in versions if v.lstrip("V").isdigit()]
            return f"V{max(nums) + 1}" if nums else "V1"
        except Exception:
            return "V1"

    # ─────────────────────── Pipeline ────────────────────────────────────────

    start_time = time.time()
    session = get_active_session()

    # Set role before any data access or DDL
    session.sql(f"USE ROLE {ROLE}").collect()
    #session.sql(f"CREATE STAGE IF NOT EXISTS {DB}.{SCHEMA}.{MODEL_NAME}").collect()
    session.sql(f"USE SCHEMA {FQN}").collect()

    # ── Step 1: Load data & temporal split ────────────────────────────────────
    # Fetch the training data from Snowflake and split by date into train and
    # test sets. The test set is held out completely — no information leaks from
    # it during training or CV.
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Loading data & temporal split ===", flush=True)

    col_list = ", ".join(ALL_COLS + ["TARGET_DATE"])
    raw_df = session.sql(f"""
        SELECT {col_list}
        FROM {TABLE} -- {FQN}.{TABLE}
        WHERE TARGET_DATE BETWEEN '{START_TRAIN_DATE}' AND '{FINISH_TEST_DATE}'
    """).to_pandas()
    raw_df.columns = [c.strip('"') for c in raw_df.columns]

    # Temporal split: train up to FINISH_TRAIN_DATE, test from START_TEST_DATE onward.
    raw_df["TARGET_DATE"] = raw_df["TARGET_DATE"].astype(str)
    train_df = raw_df[
        (raw_df["TARGET_DATE"] >= START_TRAIN_DATE) &
        (raw_df["TARGET_DATE"] <= FINISH_TRAIN_DATE)
    ].copy()
    test_df = raw_df[
        (raw_df["TARGET_DATE"] >= START_TEST_DATE) &
        (raw_df["TARGET_DATE"] <= FINISH_TEST_DATE)
    ].copy()

    print(f"[{time.time()-start_time:.1f}s]   Total rows: {len(raw_df):,}", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   Train: {len(train_df):,} ({START_TRAIN_DATE} to {FINISH_TRAIN_DATE})", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   Test:  {len(test_df):,} ({START_TEST_DATE} to {FINISH_TEST_DATE})", flush=True)

    # ── Step 2: Encode categoricals ───────────────────────────────────────────
    # Encode categoricals using pandas CategoricalDtype.
    # XGBoost handles them natively with enable_categorical=True — no one-hot needed.
    # Category vocabulary is fitted on train only so unseen test values become NaN.
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Encoding categoricals ===", flush=True)

    category_mappings = {}
    for col in CATEGORICAL_COLS:
        cat = pd.CategoricalDtype(categories=train_df[col].astype(str).unique(), ordered=False)
        category_mappings[col] = cat

    def encode(df):
        out = df[NUMERIC_FEATURE_COLS].copy().reset_index(drop=True)
        for col in CATEGORICAL_COLS:
            out[col] = pd.Categorical(df[col].astype(str).values, dtype=category_mappings[col])
        return out

    X_train = encode(train_df)
    X_test  = encode(test_df)
    y_train = train_df[TARGET_COL].values
    y_test  = test_df[TARGET_COL].values

    print(
        f"[{time.time()-start_time:.1f}s]   Feature matrix: {X_train.shape[1]} cols "
        f"({len(NUMERIC_FEATURE_COLS)} numeric + {len(CATEGORICAL_COLS)} categorical)",
        flush=True,
    )

    # ── Step 3: Train XGBRegressor with TimeSeriesSplit CV ────────────────────
    # Train XGBoost directly with fixed hyperparameters and evaluate via
    # cross_validate with TimeSeriesSplit (2 folds).
    # TimeSeriesSplit ensures training folds always precede validation folds
    # (no future leakage).
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Training XGBRegressor ===", flush=True)

    # --- Custom Scoring Functions ---
    # MAPE that ignores rows where actual demand is zero (avoids division by zero)
    def mape_nonzero(y_true, y_pred):
        mask = y_true > 0
        if not mask.any():
            return 0.0
        return float(np.mean(np.abs(y_true[mask] - y_pred[mask]) / y_true[mask]))

    # WAPE: total absolute error as a fraction of total actual demand (robust to zeros)
    def wape_scorer(y_true, y_pred):
        total = np.sum(y_true)
        if total == 0:
            return 0.0
        return float(np.sum(np.abs(y_true - y_pred)) / total)

    scoring = {
        "rmse": "neg_root_mean_squared_error",
        "mae":  "neg_mean_absolute_error",
        "mape": make_scorer(mape_nonzero, greater_is_better=False),
        "wape": make_scorer(wape_scorer, greater_is_better=False),
    }

    # Hyperparameters selected from prior grid-search sweeps:
    #   max_depth=3        : shallow trees to reduce overfitting on noisy demand data
    #   min_child_weight=5 : requires minimum 5 samples per leaf, adds regularization
    #   learning_rate=0.07 : moderate step size balancing speed vs. accuracy
    #   n_estimators=200   : number of boosting rounds (early plateau observed beyond this)
    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        max_depth=3,
        min_child_weight=5,
        learning_rate=0.07,
        n_estimators=200,
        enable_categorical=True,
        verbosity=0,
        n_jobs=-1,
    )

    cv = TimeSeriesSplit(n_splits=2)
    cv_results = cross_validate(model, X_train, y_train, cv=cv, scoring=scoring, n_jobs=1)

    # Fit final model on full training set
    model.fit(X_train, y_train)

    # sklearn returns negated scores for "neg_*" metrics, so we flip sign
    cv_rmse = -cv_results["test_rmse"].mean()
    cv_mae  = -cv_results["test_mae"].mean()
    cv_mape = -cv_results["test_mape"].mean() * 100
    cv_wape = -cv_results["test_wape"].mean() * 100

    print(f"[{time.time()-start_time:.1f}s]   CV RMSE: {cv_rmse:.4f}", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   CV MAE:  {cv_mae:.4f}", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   CV WAPE: {cv_wape:.2f}%", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   CV MAPE: {cv_mape:.2f}%", flush=True)

    # ── Step 4: Hold-out evaluation ───────────────────────────────────────────
    # Evaluate on the hold-out test set (completely unseen during training/CV)
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Hold-out evaluation ===", flush=True)

    preds   = model.predict(X_test)
    actuals = y_test

    rmse = round(float(np.sqrt(mean_squared_error(actuals, preds))), 4)
    mae  = round(float(np.mean(np.abs(actuals - preds))), 4)
    wape = round(
        float(np.sum(np.abs(actuals - preds)) / np.sum(actuals) * 100)
        if actuals.sum() > 0 else float("nan"), 4,
    )
    nonzero = actuals > 0
    mape = round(
        float(np.mean(np.abs(actuals[nonzero] - preds[nonzero]) / actuals[nonzero]) * 100)
        if nonzero.any() else float("nan"), 4,
    )

    print(f"[{time.time()-start_time:.1f}s]   RMSE: {rmse}", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   MAE:  {mae}", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   WAPE: {wape}%", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   MAPE: {mape}%", flush=True)

    # ── Step 5: Conformal calibration ─────────────────────────────────────────
    # Split conformal prediction (post-hoc, symmetric).
    # Calibrate conformity scores on the hold-out test set (asymmetric bounds
    # clamped to >= 0). The conformal quantile defines the ± interval half-width.
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Conformal calibration ===", flush=True)

    cal_preds = preds  # already computed on test set
    residuals = y_test - cal_preds
    abs_residuals = np.abs(residuals)

    n_cal = len(abs_residuals)
    conformal_quantile_level = min(1.0, (1 - ALPHA) * (1 + 1 / n_cal))
    conformal_q = float(np.quantile(abs_residuals, conformal_quantile_level))

    coverage = float(np.mean(abs_residuals <= conformal_q) * 100)
    print(f"[{time.time()-start_time:.1f}s]   Interval half-width (q): ±{conformal_q:.4f}", flush=True)
    print(f"[{time.time()-start_time:.1f}s]   Coverage on cal set: {coverage:.1f}% (target: {(1-ALPHA)*100:.0f}%)", flush=True)

    # ── Step 6: Feature importance ────────────────────────────────────────────
    # Feature importance by total gain and split count
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Feature importance ===", flush=True)

    booster   = model.get_booster()
    fi_gain   = booster.get_score(importance_type="total_gain")
    fi_split  = booster.get_score(importance_type="weight")

    fi_df = pd.DataFrame([
        {
            "FEATURE":          f,
            "IMPORTANCE_GAIN":  fi_gain.get(f, 0.0),
            "IMPORTANCE_SPLIT": float(fi_split.get(f, 0)),
            "MODEL_VERSION":    "TBD",
            "TRAINED_AT":       time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        for f in X_train.columns
    ]).sort_values("IMPORTANCE_GAIN", ascending=False).reset_index(drop=True)

    fi_df["IMPORTANCE_GAIN_PCT"]  = (fi_df["IMPORTANCE_GAIN"]  / fi_df["IMPORTANCE_GAIN"].sum()  * 100).round(2)
    fi_df["IMPORTANCE_SPLIT_PCT"] = (fi_df["IMPORTANCE_SPLIT"] / fi_df["IMPORTANCE_SPLIT"].sum() * 100).round(2)

    print("  Top 10 features (by total_gain):", flush=True)
    print(fi_df[["FEATURE", "IMPORTANCE_GAIN_PCT"]].head(10).to_string(index=False), flush=True)

    # ── Step 7: Save artifacts & register in Model Registry ───────────────────
    # Save three artifacts for the CustomModel:
    #   - calibration.pkl:       conformal_q, alpha, n_calibration
    #   - category_mappings.pkl: CategoricalDtype per categorical column (train vocab)
    #   - xgb_model.json:        the fitted XGBRegressor booster
    # These are bundled into the ModelContext and cloudpickled into the model
    # artifact. At serving time the SAME frozen encoders are loaded and used in
    # predict() — no re-fitting ever occurs, so encoding is identical between
    # training and production.
    print(f"[{time.time()-start_time:.1f}s] === {ENV}: Register in Model Registry ===", flush=True)

    reg = Registry(session=session, database_name=DB, schema_name=SCHEMA)
    model_version = next_version(reg, MODEL_NAME)

    # Update feature importance with actual version name and persist
    fi_df["MODEL_VERSION"] = model_version
    session.create_dataframe(fi_df).write.mode("append").save_as_table(
        f"{FQN}.{MODEL_NAME}_FEATURE_IMPORTANCE"
    )
    print(f"[{time.time()-start_time:.1f}s]   Feature importance saved to {FQN}.{MODEL_NAME}_FEATURE_IMPORTANCE", flush=True)

    # Save artifacts to /tmp for CustomModel registration
    artifacts_dir = "/tmp/conformal_artifacts"
    os.makedirs(artifacts_dir, exist_ok=True)

    calibration_data = {
        "conformal_q": conformal_q,
        "alpha": ALPHA,
        "n_calibration": n_cal,
    }
    with open(f"{artifacts_dir}/calibration.pkl", "wb") as f:
        pickle.dump(calibration_data, f)

    # Save category mappings so SQL inference can handle unseen categories
    with open(f"{artifacts_dir}/category_mappings.pkl", "wb") as f:
        pickle.dump(category_mappings, f)

    model.save_model(f"{artifacts_dir}/xgb_model.json")

    # Instantiate and sanity-check before registering
    conformal_instance = ConformalXGBModel(
        context=custom_model.ModelContext(
            models={},
            artifacts={
                "calibration": f"{artifacts_dir}/calibration.pkl",
                "xgb_model": f"{artifacts_dir}/xgb_model.json",
                "category_mappings": f"{artifacts_dir}/category_mappings.pkl",
            },
        )
    )

    sample_out = conformal_instance.predict(X_test.head(3))
    print(f"[{time.time()-start_time:.1f}s]   Sanity check:\n{sample_out.to_string(index=False)}", flush=True)

    # Workaround: when @remote + dbt compile the code into _udf_code.py,
    # cloudpickle deserializes ConformalXGBModel with __module__='main_module'.
    # This causes two problems:
    #   1. reg.log_model() fails with KeyError('main_module') because
    #      sys.modules['main_module'] doesn't exist.
    #   2. Even if we fix (1), cloudpickle bakes 'main_module' into the
    #      serialized artifact. At warehouse inference time, cloudpickle
    #      can't find 'main_module' and crashes with internal error 370001.
    # Fix: patch __module__ to '__main__' BEFORE log_model() so cloudpickle
    # serializes the class with '__main__' (which exists everywhere).
    import sys, types
    if 'main_module' not in sys.modules:
        sys.modules['main_module'] = types.ModuleType('main_module')
    ConformalXGBModel.__module__ = '__main__'

    # sample_input_data uses raw columns (with categoricals) — defines the
    # external calling contract for MODEL(...)!PREDICT(...) in SQL.
    reg.log_model(
        model=conformal_instance,
        model_name=MODEL_NAME,
        version_name=model_version,
        sample_input_data=X_train.head(100),
        target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
        task=task.Task.TABULAR_REGRESSION,
        metrics={
            "rmse": rmse,
            "mae": mae,
            "wape_pct": wape,
            "mape_pct": mape,
            "cv_rmse": round(cv_rmse, 4),
            "cv_mae": round(cv_mae, 4),
            "cv_wape_pct": round(cv_wape, 4),
            "cv_mape_pct": round(cv_mape, 4),
            "conformal_q": round(conformal_q, 4),
            "conformal_coverage_pct": round(coverage, 2),
        },
        comment=(
            f"{ENV} | Conformal XGBoost: predict returns [PREDICTED, PRED_LOWER_05, PRED_UPPER_95]. "
            f"Test RMSE={rmse}, MAE={mae}, WAPE={wape}%, conformal_q={conformal_q:.4f}. "
            f"Params: max_depth=3, min_child_weight=5, lr=0.07, n_estimators=200. "
            f"Train: {START_TRAIN_DATE}–{FINISH_TRAIN_DATE}, Test: {START_TEST_DATE}–{FINISH_TEST_DATE}."
        ),
    )

    print(f"[{time.time()-start_time:.1f}s]   Registered: {DB}.{SCHEMA}.{MODEL_NAME} {model_version}", flush=True)
    session.sql(f"SHOW VERSIONS IN MODEL {DB}.{SCHEMA}.{MODEL_NAME}").show()

    elapsed = time.time() - start_time
    result = {
        "version": model_version,
        "rmse": rmse,
        "mae": mae,
        "wape_pct": wape,
        "mape_pct": mape,
        "conformal_q": round(conformal_q, 4),
        "elapsed_s": round(elapsed, 1),
    }
    print(f"[{elapsed:.1f}s] DONE. ENV={ENV} version='{model_version}' RMSE={rmse} MAE={mae}", flush=True)
    return result


# ─────────────────────────────── Job runner ──────────────────────────────────

if __name__ == "__main__":
    import time
    from snowflake.snowpark.context import get_active_session

    session = get_active_session()
    session.sql(f"USE ROLE {ROLE}").collect()
    session.sql(f"USE DATABASE {DB}").collect()
    session.sql(f"USE SCHEMA {SCHEMA}").collect()
    session.sql(f"CREATE STAGE IF NOT EXISTS {DB}.{SCHEMA}.{MODEL_NAME}").collect()

    job = train()
    print(f"ML Job submitted: {job.id}")
    print(f"Initial status:   {job.status}")

    # Stream logs while the job runs
    print("\n=== STREAMING LOGS (polling every 10s) ===")
    prev_logs = ""
    while job.status in ("PENDING", "RUNNING"):
        time.sleep(10)
        try:
            current_logs = job.get_logs()
            if current_logs and current_logs != prev_logs:
                print(current_logs[len(prev_logs):], end="", flush=True)
                prev_logs = current_logs
        except Exception:
            pass
        print(f"  [...status: {job.status}]", flush=True)

    # Final log flush
    print("\n=== FINAL LOGS ===")
    try:
        final_logs = job.get_logs()
        if final_logs and final_logs != prev_logs:
            print(final_logs[len(prev_logs):], end="")
        elif not prev_logs:
            print(final_logs or "(no logs available)")
    except Exception as e:
        print(f"(could not fetch logs: {e})")

    print(f"\nFinal status: {job.status}")
    if job.status == "DONE":
        print(f"ML Job complete: {job.result()}")
    else:
        print(f"Job failed with status: {job.status}")
