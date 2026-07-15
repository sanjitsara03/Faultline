with shipments as (
    select
        shipped_at::date as date_day,
        delivered_at,
        extract(epoch from delivered_at - shipped_at) / 3600.0 as hours_to_deliver
    from {{ ref('stg_shipments') }}
)

select
    date_day,
    count(*) as shipments_count,
    round(avg(hours_to_deliver)::numeric, 2) as avg_hours_to_deliver,
    round(
        count(*) filter (where hours_to_deliver > 120)::numeric
        / nullif(count(*) filter (where delivered_at is not null), 0),
        4
    ) as pct_delivered_late
from shipments
group by date_day
order by date_day
