-- depends_on: {{ ref('snapshot_ready') }}
{{ config(materialized='view') }}
{{ listing_metrics(['listing_neighbourhood']) }}
