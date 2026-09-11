{{ config(materialized='semantic_view') }}

tables (
  BLVD_AI.ML_DEMAND_MODEL.PREDICTION_TEST
)
dimensions (
  PREDICTION_TEST.LOCATION_ID as LOCATION_ID,
  PREDICTION_TEST.SERVICE_CATEGORY as SERVICE_CATEGORY,
  PREDICTION_TEST.PREDICTION as PREDICTION,
  PREDICTION_TEST.TARGET_DATE as TARGET_DATE
)
comment='ML demand predictions by location and service category. Each row contains a predicted demand value with lower and upper confidence bounds (5th and 95th percentile) for a given location, service category, and target date.'
with extension (CA='{"tables":[{"name":"PREDICTION_TEST","dimensions":[{"name":"LOCATION_ID"},{"name":"SERVICE_CATEGORY"},{"name":"PREDICTION"}],"time_dimensions":[{"name":"TARGET_DATE"}]}]}')