-- depends_on: {{ ref('snapshot_ready') }}
{{ config(indexes=[{'columns':['lga_code'],'unique':true}]) }}
-- Static reference data does not need monthly snapshot versions.
select lga_code, lga_name from {{ ref('slv_lga') }}
