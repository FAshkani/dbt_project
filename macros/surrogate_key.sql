
{% macro surrogate_key(cols) %}
  {%- if cols | length == 0 -%}
    null
  {%- else -%}
    {%- set sep = '||' -%}

    {# Spark/Databricks, Postgres, Redshift: md5 + concat_ws #}
 
      MD5(
        CONCAT_WS(
          '{{ sep }}',
          {%- for c in cols -%}
            COALESCE(CAST({{ c }} AS string), '__NULL__')
            {%- if not loop.last -%}, {%- endif -%}
          {%- endfor -%}
        )
      )
 
  {%- endif -%}
{% endmacro %}
