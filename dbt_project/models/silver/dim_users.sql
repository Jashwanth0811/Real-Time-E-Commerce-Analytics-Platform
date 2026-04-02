{{
  config(
    materialized='table',
    schema='silver',
    indexes=[
      {'columns': ['user_id'], 'type': 'btree'}
    ]
  )
}}

/*
  dim_users: User dimension with behavioral segmentation.
  Combines silver.users base with event signals to produce
  RFM-ready attributes for analytics and ML.
*/

with user_base as (
    select
        user_id,
        first_seen,
        last_seen,
        total_sessions,
        country,
        city,
        device_type,
        updated_at
    from {{ source('silver', 'users') }}
    where user_id is not null
),

user_events as (
    select
        user_id,
        count(*)                                           as total_events,
        count(distinct event_date)                         as active_days,
        count(*) filter (where event_type='page_view')     as total_page_views,
        count(*) filter (where event_type='add_to_cart')   as total_add_to_cart,
        count(*) filter (where event_type='purchase')      as total_purchases,
        max(event_date)                                    as last_active_date,
        min(event_date)                                    as first_event_date,
        count(distinct category)                           as unique_categories_browsed
    from {{ source('silver', 'events') }}
    group by user_id
),

user_orders as (
    select
        user_id,
        count(*)                                           as total_orders,
        sum(total_amount)                                  as lifetime_value,
        avg(total_amount)                                  as avg_order_value,
        max(order_date)                                    as last_order_date,
        count(*) filter (where status='delivered')         as completed_orders,
        count(*) filter (where status in ('cancelled','returned'))
                                                           as cancelled_orders
    from {{ source('silver', 'orders') }}
    group by user_id
),

combined as (
    select
        ub.user_id,
        ub.first_seen,
        ub.last_seen,
        ub.total_sessions,
        ub.country,
        ub.city,
        ub.device_type,

        -- Event signals
        coalesce(ue.total_events,           0)  as total_events,
        coalesce(ue.active_days,            0)  as active_days,
        coalesce(ue.total_page_views,       0)  as total_page_views,
        coalesce(ue.total_add_to_cart,      0)  as total_add_to_cart,
        coalesce(ue.total_purchases,        0)  as total_purchases,
        coalesce(ue.unique_categories_browsed,0) as unique_categories_browsed,
        ue.last_active_date,

        -- Order signals
        coalesce(uo.total_orders,           0)   as total_orders,
        coalesce(uo.lifetime_value,         0)   as lifetime_value,
        coalesce(uo.avg_order_value,        0)   as avg_order_value,
        coalesce(uo.completed_orders,       0)   as completed_orders,
        coalesce(uo.cancelled_orders,       0)   as cancelled_orders,
        uo.last_order_date,

        -- Cart-to-purchase conversion
        case
            when coalesce(ue.total_add_to_cart, 0) > 0
            then round(coalesce(ue.total_purchases,0)::numeric
                       / ue.total_add_to_cart, 4)
            else 0
        end as cart_conversion_rate,

        -- Recency bucket (days since last activity)
        case
            when ue.last_active_date >= current_date - interval '7 days'  then 'active_7d'
            when ue.last_active_date >= current_date - interval '30 days' then 'active_30d'
            when ue.last_active_date >= current_date - interval '90 days' then 'active_90d'
            else 'churned'
        end as recency_bucket,

        -- Value segment
        case
            when coalesce(uo.lifetime_value, 0) >= 1000  then 'high_value'
            when coalesce(uo.lifetime_value, 0) >= 200   then 'mid_value'
            when coalesce(uo.lifetime_value, 0) >= 1     then 'low_value'
            else 'no_purchase'
        end as value_segment,

        current_timestamp as dbt_updated_at
    from user_base ub
    left join user_events ue on ub.user_id = ue.user_id
    left join user_orders uo on ub.user_id = uo.user_id
)

select * from combined
