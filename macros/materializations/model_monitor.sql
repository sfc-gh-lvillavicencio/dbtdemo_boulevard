{% materialization model_monitor, adapter='snowflake' -%}

    {# Custom materialization for Snowflake Model Monitors.
       The model body contains the WITH parameters only.
       This macro prepends CREATE MODEL MONITOR IF NOT EXISTS <name> WITH. #}

    {% set original_query_tag = set_query_tag() %}

    {% set target_relation = this.incorporate(type='view') %}
    {% set sql = model['compiled_code'] %}

    {% set create_sql %}
        CREATE MODEL MONITOR IF NOT EXISTS {{ target_relation }} WITH
        {{ sql }}
    {% endset %}

    {% call statement('main') %}
        {{ create_sql }}
    {% endcall %}

    {% do unset_query_tag(original_query_tag) %}
    {% do return({'relations': [target_relation]}) %}

{%- endmaterialization %}
