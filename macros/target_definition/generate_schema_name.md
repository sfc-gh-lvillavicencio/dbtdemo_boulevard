

{% docs generate_schema_name %}

This overwrites the default dbt macros. more info here : https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas

default schema name on CICD:

    set result_schema_name = "dbt_cloud_pr_" ~ env_var('DBT_CLOUD_JOB_ID','UNKNOWN') ~ "_" ~ env_var('DBT_CLOUD_PR_ID','UNKNOWN') | trim

it doesnt work when we have model with similar name but in different schemas (RAW, INTERGATION)

{% enddocs %}
