with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select
        order_id,
        sum(amount) as order_revenue,
        count(*) as payment_count
    from {{ ref('stg_payments') }}
    group by order_id
),

shipments as (
    select * from {{ ref('stg_shipments') }}
),

refunds as (
    select
        order_id,
        sum(amount) as refund_amount
    from {{ ref('stg_refunds') }}
    group by order_id
)

select
    orders.order_id,
    orders.customer_id,
    orders.ordered_at,
    orders.status,
    orders.currency,
    payments.order_revenue,
    payments.payment_count,
    shipments.shipment_id,
    shipments.carrier,
    shipments.shipping_cost,
    shipments.shipped_at,
    shipments.delivered_at,
    refunds.refund_amount
from orders
left join payments on orders.order_id = payments.order_id
left join shipments on orders.order_id = shipments.order_id
left join refunds on orders.order_id = refunds.order_id
