{{ config(
    materialized = 'incremental'
) }}

select
    order_id,
    shop_id,
    amount,
    created_at
from {{ ref('orders') }}  -- さっきの view を参照する

{% if is_incremental() %}
  -- 2回目以降: 既に入っている最大 created_at より新しいものだけ
  where created_at > (
    select coalesce(max(created_at), '1900-01-01')
    from {{ this }}
  )
{% endif %}

