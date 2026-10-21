{{ config(pre_hook="{{ snapshot_checks() }}") }}
-- depends_on: {{ ref('slv_census_g01') }}
-- depends_on: {{ ref('slv_census_g02') }}
-- depends_on: {{ ref('slv_suburb') }}
-- depends_on: {{ ref('host_snapshot_input') }}
-- depends_on: {{ ref('slv_listings') }}
-- depends_on: {{ ref('property_snapshot_input') }}
-- depends_on: {{ ref('slv_lga') }}
-- Shared dependency: all Silver tests must pass before either snapshot.
select {{ observation_month() }} as observation_month
