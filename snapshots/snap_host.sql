{% snapshot snap_host %}
{{ config(
    schema='silver',
    strategy='timestamp',
    unique_key='host_id',
    updated_at='updated_at',
    hard_deletes='ignore',
    tags=['entity_snapshot'],
    pre_hook="{{ snapshot_checks() }}",
    post_hook="{{ snapshot_checks('host') }}"
) }}
-- Each newer observed month creates a version; missing hosts are not deletions.
-- depends_on: {{ ref('silver_ready') }}
select {{ entity_columns('host') }}
from {{ ref('host_snapshot_input') }}
{% endsnapshot %}
