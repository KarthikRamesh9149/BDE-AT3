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

{% macro observation_month_value() -%}
  {%- set month = var('observation_month', none) -%}
  {%- set allowed = ['2020-05-01','2020-06-01','2020-07-01','2020-08-01',
                     '2020-09-01','2020-10-01','2020-11-01','2020-12-01',
                     '2021-01-01','2021-02-01','2021-03-01','2021-04-01'] -%}
  {%- if month is none and not execute -%}
    {# Parsing discovers dependencies without querying the database. #}
    {{ return('2020-05-01') }}
  {%- endif -%}
  {%- if month is none -%}
    {%- if target.type != 'postgres' or target.database != 'BDEAT3' -%}
      {{ exceptions.raise_compiler_error('Use PostgreSQL database BDEAT3.') }}
    {%- endif -%}
    {# Reuse the start hook's recorded month throughout a deployment build. #}
    {%- if flags.WHICH == 'build' and target.schema == 'bdeat3' -%}
      {%- set attempts = run_query("select to_regclass('pipeline.dbt_attempts') is not null") -%}
      {%- if attempts.rows[0][0] -%}
        {%- set pinned = run_query("select observation_month::text from pipeline.dbt_attempts where invocation_id='" ~ invocation_id ~ "'") -%}
        {%- if pinned.rows|length == 1 -%}
          {%- set month = pinned.rows[0][0] -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
    {%- if month is none -%}
      {# Select the latest committed load first; never fall back past an unverified one. #}
      {%- set latest_query -%}
        select m.observation_month::text,
               m.verified_at is not null and r.verified_at is not null
                 and r.observation_month=m.observation_month as verified
        from pipeline.monthly_loads m
        left join pipeline.load_runs r using(run_id)
        order by m.observation_month desc limit 1
      {%- endset -%}
      {%- set latest = run_query(latest_query) -%}
      {%- if latest.rows|length != 1 or not latest.rows[0][1] -%}
        {{ exceptions.raise_compiler_error('Automatic month selection requires the latest Bronze load to be verified by Airflow.') }}
      {%- endif -%}
      {%- set month = latest.rows[0][0] -%}
    {%- endif -%}
  {%- endif -%}
  {%- if month not in allowed -%}
    {{ exceptions.raise_compiler_error('Supply observation_month as YYYY-MM-01, May 2020 to April 2021.') }}
  {%- endif -%}
  {{ return(month) }}
{%- endmacro %}

{% macro observation_month() -%}
  {%- set month = observation_month_value() -%}
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
