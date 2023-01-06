{{ config(materialized='table') }}

with 
transactions_ as (
  select 
    bh.parent_id as account_id
    
    -- TODO: for the purposes of WiFi+POS there should be a view on zenprod.pos.merchant & zenprod.pos.purchase tables
    --       that handle some of these manipulations.
    -- TODO: Ensure the pos_payment_method logic is consistently applied and covers all edge cases. 
    -- Apple Valley Stores has four distinct POS Clover but only on WiFi location
    -- Map all mart_merged_presence_pos__transactions to the General Store = 5a8f0040fce4e2000bce6996
    , case when m.zenreach_bid in ('5fbc36576d020c00015e354a', '5fbc36566d020c00015e3549', '5fbc3657d70d5f0001e69f9a', '5a8f0040fce4e2000bce6996') then '5a8f0040fce4e2000bce6996'
           else m.zenreach_bid end as business_id
    , p.foreign_created_time as pos_time_utc
    -- in the absence of a utc offset, assume chicago which is middle-ish right?
    , dateadd(hours, ifnull(g.timezone_utc_offset, -6), p.foreign_created_time) as pos_time
    , p.total as pos_amount
    , p.customer_id as pos_customer_id
    , case when p.customer_id = '00000000-0000-0000-0000-000000000000' then 'CASH'
           when lower(c.name) like 'gh %' then 'APP' -- TODO: this should be corrected to use regex to find 'GH 123456'
           when lower(c.name) like 'doordash' then 'APP'
           when lower(c.name) like 'postmate%' then 'APP'
           when lower(c.name) like '%, llc' then 'BUSINESS CARD'
           when lower(c.name) like '% llc' then 'BUSINESS CARD'
           when lower(c.name) in ('a gift for you', 'gift for you', 'thank you', 'yelp prepaid', 'prepaid', 'regions prepaid') then 'PREPAID CARD'
           when lower(c.name) is NULL or lower(c.name) = '' then 'CASH' -- TODO: investigate why this happens?
           else 'CARD'
           END pos_payment_method
    , c.name as name_orig
    , clean_name(c.name) as pos_name
    , iff(regexp_replace(c.primary_email,'[^a-zA-Z0-9@]','') = '' or c.primary_email not like '%@%'
          , NULL
          , clean_contact(c.primary_email)) as pos_email
    , p.id as pos_purchase_id
  
    from {{ seed_or_ref( ref('stg_pos__purchase'), 'seed_purchase') }} p
      join {{ seed_or_ref( ref('stg_pos__merchant'), 'seed_merchant') }} m
        on p.merchant_id = m.id
      left join {{ seed_or_ref( ref('stg_pos__customer'), 'seed_customer') }} c

    on p.customer_id = c.id
       and m.id = c.merchant_id
  -- TODO: if a merchant is not geocoded, default to Chicago (middle of the country)
  -- this isn't a good plan.
  
    left join {{ seed_or_ref( ref('stg_business_profiles__d_business_geocode'), 'seed_d_business_geocode') }} g
      on m.zenreach_bid = g.business_id
    left join {{ seed_or_ref( ref('stg_crm__businessprofile_hierarchy'), 'seed_businessprofile_hierarchy') }} bh on m.zenreach_bid = bh.business_id
  where 
      status = 'COMPLETED_STATUS'
)
, all_transactions_ as (
    (select 
    account_id
      , business_id
      , pos_time_utc
      , pos_time
      , pos_payment_method
      , pos_amount
      , pos_customer_id
      , pos_name
      , pos_email
      , pos_purchase_id
      , count(*) over (partition by business_id, pos_customer_id) as pos_tx_count
from transactions_)
)
select *, current_timestamp() as created_at
from all_transactions_
