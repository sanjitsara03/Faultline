select
    shipment_id,
    order_id,
    carrier,
    shipping_cost::numeric(6, 2) as shipping_cost,
    shipped_at::timestamp as shipped_at,
    delivered_at::timestamp as delivered_at
from {{ source('raw', 'raw_shipments') }}
