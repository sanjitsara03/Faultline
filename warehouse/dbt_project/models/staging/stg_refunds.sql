select
    refund_id,
    order_id,
    amount::numeric(10, 2) as amount,
    reason,
    refunded_at::timestamp as refunded_at
from {{ source('raw', 'raw_refunds') }}
