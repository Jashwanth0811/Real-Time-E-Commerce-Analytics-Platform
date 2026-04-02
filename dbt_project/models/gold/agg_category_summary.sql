{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    e.event_date                                            as report_date,
    e.category,
    count(*) filter (where e.event_type='page_view')        as total_views,
    count(*) filter (where e.event_type='add_to_cart')      as total_add_to_cart,
    count(*) filter (where e.event_type='purchase')         as total_purchases,
    count(distinct e.user_id)                               as unique_users,
    count(distinct e.product_id)                            as unique_products_viewed,
    sum(e.price * e.quantity)
        filter (where e.event_type='purchase')              as total_revenue,
    avg(e.price) filter (where e.event_type='purchase')     as avg_purchase_price,
    case
        when count(*) filter (where e.event_type='page_view') > 0
        then round(
            count(*) filter (where e.event_type='purchase')::numeric
            / count(*) filter (where e.event_type='page_view'), 4)
        else 0
    end                                                     as category_conversion_rate,
    current_timestamp                                       as dbt_updated_at
from {{ source('silver', 'events') }} e
where e.category is not null
group by e.event_date, e.category
order by report_date, total_revenue desc nulls last
