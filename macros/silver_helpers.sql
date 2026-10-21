-- Deployment uses explicit medallion schemas; Studio uses isolated developer schemas.
{% macro warehouse_schema(layer) -%}
  {% if target.type != 'postgres' or target.database != 'BDEAT3' %}
    {{ exceptions.raise_compiler_error('Use PostgreSQL database BDEAT3.') }}
  {% endif %}
  {{ return(layer if target.schema == 'bdeat3' else target.schema ~ '_' ~ layer) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ return(warehouse_schema(custom_schema_name|trim) if custom_schema_name else target.schema) }}
{%- endmacro %}

{% macro snapshot_relation(kind) -%}
  {{ return(adapter.quote(warehouse_schema('silver')) ~ '.' ~ adapter.quote('snap_' ~ kind)) }}
{%- endmacro %}

{% macro observation_month() -%}
  {%- set month = var('observation_month', none) -%}
  {%- set allowed = ['2020-05-01','2020-06-01','2020-07-01','2020-08-01',
                     '2020-09-01','2020-10-01','2020-11-01','2020-12-01',
                     '2021-01-01','2021-02-01','2021-03-01','2021-04-01'] -%}
  {%- if month is none and not execute -%}
    {# Parse-only placeholder; execution always requires the explicit variable. #}
    {{ return("date '2020-05-01'") }}
  {%- elif month not in allowed -%}
    {{ exceptions.raise_compiler_error('Supply observation_month as YYYY-MM-01, May 2020 to April 2021.') }}
  {%- endif -%}
  {{ return("date '" ~ month ~ "'") }}
{%- endmacro %}

{% macro geography_key(column) -%}
  upper(btrim(regexp_replace({{ column }}, '\s+', ' ', 'g')))
{%- endmacro %}

{% macro source_flag(column) -%}
  case btrim({{ column }})
    when 't' then true
    when 'f' then false
    when '' then null
    else case when {{ column }} is null then null
         else ('invalid t/f flag: ' || {{ column }})::boolean end
  end
{%- endmacro %}

{% macro host_registration_date(column) -%}
  case when nullif(btrim({{ column }}), '') is null then null
       when btrim({{ column }}) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       then make_date(split_part(btrim({{ column }}), '/', 3)::integer,
                      split_part(btrim({{ column }}), '/', 2)::integer,
                      split_part(btrim({{ column }}), '/', 1)::integer)
       else ('invalid day/month/year: ' || {{ column }})::date end
{%- endmacro %}

{% test nonnull_fields(model, fields) %}
select * from {{ model }} where
{% for field in fields %}
  {{ adapter.quote(field) }} is null{% if not loop.last %} or{% endif %}
{% endfor %}
{% endtest %}

{% test unique_fields(model, fields) %}
select {% for field in fields %}{{ adapter.quote(field) }}{% if not loop.last %}, {% endif %}{% endfor %}
from {{ model }}
group by {% for field in fields %}{{ adapter.quote(field) }}{% if not loop.last %}, {% endif %}{% endfor %}
having count(*) > 1
{% endtest %}
