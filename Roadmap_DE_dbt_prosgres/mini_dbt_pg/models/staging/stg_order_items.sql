{{ config(materialized = 'table') }}

with source as (

    select *
    from {{ source('raw', 'raw_order_items_dataset') }}

),


casted as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_at,
        price,
        freight_value
    from source

)

select *
from casted