select
    nullif(btrim(lga_code), '') as lga_code,
    nullif(btrim(lga_name), '') as lga_name,
    {{ geography_key('lga_name') }} as lga_name_key,
    _source_file,
    _loaded_at
from {{ source('bronze', 'raw_lga_code') }}
