{{ config(materialized='view') }}

select
    order_id,
    shop_id,
    amount,
    created_at
from read_csv_auto('data/orders.csv')

