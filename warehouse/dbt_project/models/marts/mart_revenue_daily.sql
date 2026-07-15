select
    ordered_at::date as date_day,
    count(*) as orders_count,
    sum(coalesce(order_revenue, 0)) as total_revenue,
    sum(coalesce(refund_amount, 0)) as refunds_amount,
    sum(coalesce(order_revenue, 0)) - sum(coalesce(refund_amount, 0)) as net_revenue,
    round(sum(coalesce(order_revenue, 0)) / count(*), 2) as aov
from {{ ref('int_orders_joined') }}
group by date_day
order by date_day
