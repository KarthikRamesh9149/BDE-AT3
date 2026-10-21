select
    nullif(btrim(s.suburb_name), '') as suburb_name,
    {{ geography_key('s.suburb_name') }} as suburb_key,
    nullif(btrim(s.lga_name), '') as source_lga_name,
    l.lga_code,
    l.lga_name,
    s._source_file,
    s._loaded_at
from {{ source('bronze', 'raw_lga_suburb') }} s
left join {{ ref('slv_lga') }} l
    on {{ geography_key('s.lga_name') }} = l.lga_name_key
