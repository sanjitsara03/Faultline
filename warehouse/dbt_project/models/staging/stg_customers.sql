select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    created_at::timestamp as created_at
from {{ source('raw', 'raw_customers') }}
