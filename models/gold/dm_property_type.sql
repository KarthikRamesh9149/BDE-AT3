-- depends_on: {{ ref('snapshot_ready') }}
{{ config(materialized='view') }}
{{ listing_metrics(['property_type','room_type','accommodates']) }}
