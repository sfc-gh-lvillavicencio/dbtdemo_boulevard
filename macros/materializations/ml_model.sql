{% materialization ml_model, adapter='snowflake', supported_languages=['sql', 'python'] -%}

    {# Custom materialization for ML model training pipelines.
       Currently replicates the built-in Snowflake table materialization.
       Provides a distinct materialization type for ML models, enabling
       future customization (e.g., model registry integration, metrics
       logging, artifact tracking) without affecting standard table models. #}

    {% set original_query_tag = set_query_tag() %}

    {%- set identifier = model['alias'] -%}
    {%- set language = model['language'] -%}

    {% set grant_config = config.get('grants') %}
    {% set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
    {% set target_relation = api.Relation.create(
        identifier=identifier,
        schema=schema,
        database=database,
        type='table'
    ) %}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall %}

    {{ run_hooks(post_hooks) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {% do unset_query_tag(original_query_tag) %}
    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
