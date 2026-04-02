{{
  config(
    materialized='table',
    schema='silver',
    indexes=[
      {'columns': ['order_date'], 'type': 'btree'},
      {'columns': ['user_id'],    'type': 'btree'},
      {'columns': ['status'],     'type': 'btree'},
    ]
  )
}}

/*
  fct_orders: Central fact table for all e-commerce orders.
  Enriches silver.orders with computed fields and joins
  user signals from silver.events.
*/

with orders as (
    select
        order_id,
        user_id,
        status,
        total_amount,
        discount_pct,
        payment_method,
        item_count,
        order_date,
        created_at,
        updated_at,
        processed_at
    from {{ source('silver', 'orders') }}
    where order_id is not null
      and user_id  is not null
      and total_amount >= 0
),

order_events as (
    select
        user_id,
        event_date,
        count(*) filter (where event_type = 'page_view')    as session_page_views,
        count(*) filter (where event_type = 'add_to_cart')  as pre_purchase_carts,
        min(ts)                                             as session_first_touch,
        max(ts)                                             as session_last_touch
    from {{ source('silver', 'events') }}
    group by user_id, event_date
),

enriched as (
    select
        o.order_id,
        o.user_id,
        o.status,
        o.total_amount                                       as gross_amount,
        round(o.total_amount * (1 - coalesce(o.discount_pct,0)/100), 2)
                                                            as net_amount,
        o.discount_pct,
        o.payment_method,
        o.item_count,
        o.order_date,
        o.created_at,
        o.updated_at,
        -- Derived flags
        case when o.status = 'delivered'  then true else false end as is_completed,
        case when o.status in ('cancelled','returned')
                                          then true else false end as is_cancelled,
        case when o.discount_pct > 0      then true else false end as had_discount,
        case when o.payment_method = 'cod' then true else false end as is_cod,
        -- Day of week for seasonality analysis
        extract(dow from o.order_date)::int                 as day_of_week,
        extract(month from o.order_date)::int               as order_month,
        extract(year  from o.order_date)::int               as order_year,
        -- Pre-purchase engagement (join on user + date)
        coalesce(e.session_page_views, 0)                   as pre_purchase_page_views,
        coalesce(e.pre_purchase_carts, 0)                   as pre_purchase_carts,
        current_timestamp                                   as dbt_updated_at
    from orders o
    left join order_events e
           on o.user_id    = e.user_id
          and o.order_date = e.event_date
)

select * from enriched
