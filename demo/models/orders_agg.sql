{{ config(materialized='table') }}

select
    shop_id,
    count(*) as order_count,
    sum(amount) as total_amount
from {{ ref('orders') }}
group by 1

