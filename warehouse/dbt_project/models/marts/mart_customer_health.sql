-- Grain: one row per calendar day of ordered_at.

with orders as (
    select * from {{ ref('int_orders_joined') }}
),

first_orders as (
    select
        customer_id,
        first_order_at::date as first_order_day
    from {{ ref('int_customer_ltv') }}
),

daily as (
    select
        orders.ordered_at::date as date_day,
        count(distinct case
            when orders.ordered_at::date = first_orders.first_order_day
                then orders.customer_id
        end) as new_customers,
        count(distinct orders.customer_id) as active_customers,
        sum(coalesce(orders.order_revenue, 0)) as revenue
    from orders
    left join first_orders on orders.customer_id = first_orders.customer_id
    group by 1
)

select
    date_day,
    new_customers,
    active_customers,
    round(revenue / active_customers, 2) as revenue_per_active
from daily
order by date_day
