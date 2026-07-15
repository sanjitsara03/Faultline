select
    customer_id,
    sum(coalesce(order_revenue, 0)) as lifetime_revenue,
    count(*) as order_count,
    min(ordered_at) as first_order_at,
    max(ordered_at) as last_order_at
from {{ ref('int_orders_joined') }}
group by customer_id
