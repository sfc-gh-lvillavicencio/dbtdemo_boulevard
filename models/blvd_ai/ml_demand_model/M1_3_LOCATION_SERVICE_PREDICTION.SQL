{{
    config(
        materialized='incremental',
        unique_key=['LOCATION_ID', 'SERVICE_CATEGORY', 'PREDICTION_TIMESTAMP'],
        incremental_strategy='merge'
    )
}}

-- Inference: score using the registered model.
-- Flattened prediction columns + 90% conformal prediction interval.
-- Incremental: only scores dates not already in the target table.
WITH raw_predictions AS (
    SELECT
        LOCATION_ID,
        SERVICE_CATEGORY,
        TARGET_DATE::TIMESTAMP_NTZ AS PREDICTION_TIMESTAMP,
        PROVIDER_HOURS_DEMANDED AS ACTUAL,
        MODEL({{ ref('M1_2_LOCATION_SERVICE_MODEL') }}, "LAST")!"PREDICT"(
            DAY_OF_WEEK,
            DAY_OF_MONTH,
            WEEK_OF_YEAR,
            DAY_OF_YEAR,
            IS_WEEKEND,
            IS_HOLIDAY,
            DAYS_TO_NEAREST_HOLIDAY,
            LOCATION_AGE_DAYS,
            LOCATION_STAFF_COUNT,
            COUNT_APPTS_L7D,
            COUNT_APPTS_L30D,
            COUNT_APPTS_DOW_AVG,
            COUNT_APPTS_LAG_7D,
            COUNT_APPTS_LAG_14D,
            COUNT_APPTS_LAG_28D,
            COUNT_APPTS_LAG_364D,
            COUNT_APPTS_TREND_4W,
            HOURS_PROVIDER_L7D,
            HOURS_PROVIDER_L30D,
            HOURS_PROVIDER_DOW_AVG,
            HOURS_PROVIDER_LAG_7D,
            HOURS_PROVIDER_LAG_14D,
            HOURS_PROVIDER_LAG_28D,
            HOURS_PROVIDER_LAG_364D,
            HOURS_PROVIDER_TREND_4W,
            HOURS_PROVIDER_PER_APPT_L30D,
            AVG_LEAD_TIME_DAYS,
            EMAIL_MSGS_L30D,
            SMS_MSGS_L7D,
            SF_ACCOUNT_TENURE,
            SERVICE_CATEGORY,
            LOCATION_CITY,
            SF_SEGMENT,
            SF_INDUSTRY
        ) AS PREDICTION
    FROM {{ ref('M1_1_RAW_TRAINING_DATA') }}
    WHERE PROVIDER_HOURS_DEMANDED IS NOT NULL
    {% if is_incremental() %}
        AND TARGET_DATE > (SELECT MAX(PREDICTION_TIMESTAMP)::DATE FROM {{ this }})
    {% else %}
        AND TARGET_DATE BETWEEN '2026-04-01' AND  '2026-04-30'
    {% endif %}
)
SELECT
    LOCATION_ID,
    SERVICE_CATEGORY,
    PREDICTION_TIMESTAMP,
    ACTUAL,
    PREDICTION:PREDICTED::FLOAT AS PREDICTED,
    PREDICTION:PRED_LOWER_05::FLOAT AS PRED_LOWER_05,
    PREDICTION:PRED_UPPER_95::FLOAT AS PRED_UPPER_95
FROM raw_predictions
