{{
  config(
    materialized='table',
    schema='gold',
    indexes=[
      {'columns': ['report_date', 'product_id'], 'type': 'btree'}
    ]
  )
}}

/*
  agg_product_funnel: Product-level funnel metrics.
  Shows view → add_to_cart → purchase conversion per product per day.
*/

with product_events as (
    select
        event_date                                                      as report_date,
        product_id,
        category,
        count(*) filter (where event_type = 'page_view')               as views,
        count(distinct user_id) filter (where event_type='page_view')  as unique_viewers,
        count(*) filter (where event_type = 'add_to_cart')             as add_to_cart,
        count(distinct user_id)
            filter (where event_type = 'add_to_cart')                  as unique_carted,
        count(*) filter (where event_type = 'purchase')                as purchases,
        count(distinct user_id)
            filter (where event_type = 'purchase')                     as unique_buyers,
        avg(price) filter (where event_type = 'purchase')              as avg_sale_price,
        sum(price * quantity) filter (where event_type = 'purchase')   as revenue
    from {{ source('silver', 'events') }}
    where product_id is not null
    group by event_date, product_id, category
)

select
    report_date,
    product_id,
    category,
    coalesce(views,          0) as views,
    coalesce(unique_viewers, 0) as unique_viewers,
    coalesce(add_to_cart,    0) as add_to_cart,
    coalesce(unique_carted,  0) as unique_carted,
    coalesce(purchases,      0) as purchases,
    coalesce(unique_buyers,  0) as unique_buyers,
    coalesce(avg_sale_price, 0) as avg_sale_price,
    coalesce(revenue,        0) as revenue,

    -- Funnel rates
    case
        when coalesce(views, 0) > 0
        then round(coalesce(add_to_cart,0)::numeric / views, 4)
        else 0
    end as view_to_cart_rate,

    case
        when coalesce(add_to_cart, 0) > 0
        then round(coalesce(purchases,0)::numeric / add_to_cart, 4)
        else 0
    end as cart_to_purchase_rate,

    case
        when coalesce(views, 0) > 0
        then round(coalesce(purchases,0)::numeric / views, 4)
        else 0
    end as overall_conversion_rate,

    current_timestamp as dbt_updated_at
from product_events
order by report_date, revenue desc
