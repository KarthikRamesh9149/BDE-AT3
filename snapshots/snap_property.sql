{% snapshot snap_property %}
{{ config(
    schema='silver',
    strategy='timestamp',
    unique_key='listing_id',
    updated_at='updated_at',
    hard_deletes='ignore',
    tags=['entity_snapshot'],
    pre_hook="{{ snapshot_checks() }}",
    post_hook="{{ snapshot_checks('property') }}"
) }}
-- Host ownership and property attributes are historical; measures stay in the fact.
-- depends_on: {{ ref('silver_ready') }}
select {{ entity_columns('property') }}
from {{ ref('property_snapshot_input') }}
{% endsnapshot %}
