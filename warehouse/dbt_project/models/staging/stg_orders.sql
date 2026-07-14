select
    order_id,
    customer_id,
    ordered_at::timestamp as ordered_at,
    status,
    currency,
    _loaded_at::timestamp as loaded_at
from {{ source('raw', 'raw_orders') }}
