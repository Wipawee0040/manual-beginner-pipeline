{{ config(materialized='table') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_at,
    price,
    freight_value,
    price + freight_value as total_item_value
from {{ ref('stg_order_items') }}