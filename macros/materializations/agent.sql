{% materialization agent, adapter='snowflake' -%}

    {# Custom materialization for Snowflake Cortex Agents.
       The model body is the DDL body after CREATE OR REPLACE AGENT <name>.
       This macro prepends the CREATE OR REPLACE AGENT statement. #}

    {% set original_query_tag = set_query_tag() %}

    {% set target_relation = this.incorporate(type='view') %}
    {% set sql = model['compiled_code'] %}

    {% set create_sql %}
        CREATE OR REPLACE AGENT {{ target_relation }}
        {{ sql }}
    {% endset %}

    {% call statement('main') %}
        {{ create_sql }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% do unset_query_tag(original_query_tag) %}
    {% do return({'relations': [target_relation]}) %}

{%- endmaterialization %}
