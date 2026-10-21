{{ config(pre_hook="{{ gold_checks() }}") }}
-- depends_on: {{ ref('snap_host') }}
-- depends_on: {{ ref('snap_property') }}
-- depends_on: {{ ref('slv_census_g01') }}
-- depends_on: {{ ref('slv_census_g02') }}
-- depends_on: {{ ref('slv_suburb') }}
-- depends_on: {{ ref('host_snapshot_input') }}
-- depends_on: {{ ref('slv_listings') }}
-- depends_on: {{ ref('property_snapshot_input') }}
-- depends_on: {{ ref('slv_lga') }}
-- Both snapshot histories and their tests precede every Gold branch.
select {{ observation_month() }} as observation_month
