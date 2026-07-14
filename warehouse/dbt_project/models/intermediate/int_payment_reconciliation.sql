-- Grain: one row per order. Data-quality flags on the money columns.

select
    order_id,
    status,
    order_revenue,
    payment_count,
    refund_amount,
    order_revenue is null as no_successful_payment,
    -- money captured for a cancelled order, or a live order with nothing captured
    (status = 'cancelled' and order_revenue is not null)
        or (status != 'cancelled' and order_revenue is null) as payment_status_mismatch,
    coalesce(refund_amount, 0) > coalesce(order_revenue, 0) as refund_exceeds_revenue
from {{ ref('int_orders_joined') }}
