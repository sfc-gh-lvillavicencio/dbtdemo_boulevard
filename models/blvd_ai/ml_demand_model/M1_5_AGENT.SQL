{{ config(materialized='agent') }}

  COMMENT = 'Agent for querying ML demand predictions by location and service category.'
  PROFILE = '{"display_name": "ML Demand Predictions"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  instructions:
    response: "Answer concisely using bullet points where appropriate."
    orchestration: "Use the demand_predictions tool for any question about predicted demand, forecasts, or predictions by location or service category."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "demand_predictions"
        description: "Query ML demand predictions by location and service category, including predicted values and confidence intervals."

  tool_resources:
    demand_predictions:
      execution_environment:
        type: "warehouse"
        warehouse: "SNOWFLAKE_PS_STANDARD"
      semantic_view: "{{ ref('M1_4_SEMANTIC_VIEW') }}"
  $$
