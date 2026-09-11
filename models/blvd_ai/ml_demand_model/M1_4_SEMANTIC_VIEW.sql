{{ config(materialized='semantic_view') }}

TABLES (
    PREDICTION_TEST AS {{ ref('M1_3_LOCATION_SERVICE_PREDICTION') }}
)

DIMENSIONS (
    PREDICTION_TEST.LOCATION_ID AS PREDICTION_TEST.LOCATION_ID,
    PREDICTION_TEST.SERVICE_CATEGORY AS PREDICTION_TEST.SERVICE_CATEGORY,
    PREDICTION_TEST.PREDICTION AS PREDICTION_TEST.PREDICTION,
    PREDICTION_TEST.TARGET_DATE AS PREDICTION_TEST.TARGET_DATE
)
comment='ML demand predictions by location and service category. Each row contains a predicted demand value with lower and upper confidence bounds (5th and 95th percentile) for a given location, service category, and target date.'
with extension (CA='{"tables":[{"name":"PREDICTION_TEST","dimensions":[{"name":"LOCATION_ID"},{"name":"SERVICE_CATEGORY"},{"name":"PREDICTION"}],"time_dimensions":[{"name":"TARGET_DATE"}]}]}')