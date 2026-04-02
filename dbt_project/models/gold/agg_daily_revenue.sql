{{
  config(
    materialized='table',
    schema='gold',
    indexes=[
      {'columns': ['report_date'], 'type': 'btree'}
    ]
  )
}}

/*
  agg_daily_revenue: Gold-layer daily business summary.
  Used by executive dashboards and BI tools.
*/

with daily_orders as (
    select
        order_date                                        as report_date,
        count(*)                                          as total_orders,
        count(*) filter (where is_completed)              as completed_orders,
        count(*) filter (where is_cancelled)              as cancelled_orders,
        sum(gross_amount)                                 as gross_revenue,
        sum(net_amount)                                   as net_revenue,
        avg(net_amount)                                   as avg_order_value,
        count(distinct user_id)                           as unique_buyers,
        sum(item_count)                                   as total_items_sold,
        count(*) filter (where had_discount)              as discounted_orders,
        avg(discount_pct) filter (where had_discount)     as avg_discount_pct,
        sum(net_amount) filter (where payment_method='cod')
                                                          as cod_revenue,
        sum(net_amount) filter (where payment_method='credit_card')
                                                          as cc_revenue
    from {{ ref('fct_orders') }}
    where order_date is not null
    group by order_date
),

daily_events as (
    select
        event_date                                        as report_date,
        count(distinct session_id)                        as total_sessions,
        count(distinct user_id)                           as unique_visitors,
        count(*) filter (where event_type='page_view')    as page_views,
        count(*) filter (where event_type='add_to_cart')  as add_to_cart_events,
        count(distinct user_id)
            filter (where event_type='add_to_cart')       as users_who_added_to_cart
    from {{ source('silver', 'events') }}
    group by event_date
),

combined as (
    select
        o.report_date,
        coalesce(o.total_orders,       0) as total_orders,
        coalesce(o.completed_orders,   0) as completed_orders,
        coalesce(o.cancelled_orders,   0) as cancelled_orders,
        coalesce(o.gross_revenue,      0) as gross_revenue,
        coalesce(o.net_revenue,        0) as net_revenue,
        coalesce(o.avg_order_value,    0) as avg_order_value,
        coalesce(o.unique_buyers,      0) as unique_buyers,
        coalesce(o.total_items_sold,   0) as total_items_sold,
        coalesce(o.discounted_orders,  0) as discounted_orders,
        coalesce(o.avg_discount_pct,   0) as avg_discount_pct,
        coalesce(o.cod_revenue,        0) as cod_revenue,
        coalesce(o.cc_revenue,         0) as cc_revenue,

        -- Traffic signals
        coalesce(e.total_sessions,          0) as total_sessions,
        coalesce(e.unique_visitors,         0) as unique_visitors,
        coalesce(e.page_views,              0) as page_views,
        coalesce(e.add_to_cart_events,      0) as add_to_cart_events,
        coalesce(e.users_who_added_to_cart, 0) as users_who_added_to_cart,

        -- Funnel conversion
        case
            when coalesce(e.unique_visitors, 0) > 0
            then round(coalesce(o.unique_buyers, 0)::numeric
                       / e.unique_visitors, 4)
            else 0
        end as visitor_to_buyer_rate,

        case
            when coalesce(e.users_who_added_to_cart, 0) > 0
            then round(coalesce(o.unique_buyers, 0)::numeric
                       / e.users_who_added_to_cart, 4)
            else 0
        end as cart_to_purchase_rate,

        -- Revenue per visitor
        case
            when coalesce(e.unique_visitors, 0) > 0
            then round(coalesce(o.net_revenue, 0)::numeric
                       / e.unique_visitors, 2)
            else 0
        end as revenue_per_visitor,

        current_timestamp as dbt_updated_at
    from daily_orders o
    full outer join daily_events e on o.report_date = e.report_date
)

select * from combined
order by report_date
