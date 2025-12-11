{{ config(
    materialized = 'incremental'
) }}

select
    order_id,
    shop_id,
    amount,
    created_at
from read_csv_auto('data/orders.csv')

{% if is_incremental() %}
  -- 2回目以降の dbt run のときだけ有効になる条件
  where created_at > (
    select coalesce(max(created_at), '1900-01-01')
    from {{ this }}
  )
{% endif %}

