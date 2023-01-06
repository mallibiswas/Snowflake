{{ config(materialized='table') }}
with pos_customers_ as (
  select 
    distinct 
        business_id
        , pos_customer_id
        , clean_name(pos_name) as pos_name
        , get_initials(pos_name) as pos_initials
        , get_last_name(pos_name) as pos_last_name
        , pos_email
        , count(*) as pos_tx_count
  from {{ref('mart_merged_presence_pos__transactions')}} t
  where pos_payment_method = 'CARD' -- only attempt matches on CC mart_merged_presence_pos__transactions
        and pos_name is not null
        -- This warehouse has a 60 minute run time limit, cutting back to 30 days back
        and pos_time >= dateadd(days, -31, current_date()) -- all time '2019-01-01'
  group by 1,2,3,4,5,6
)
select *, current_timestamp() as created_at
from pos_customers_
