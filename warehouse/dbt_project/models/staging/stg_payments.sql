select
    payment_id,
    order_id,
    amount::numeric(10, 2) as amount,
    payment_method,
    status,
    paid_at::timestamp as paid_at
from {{ source('raw', 'raw_payments') }}
where status = 'success'
