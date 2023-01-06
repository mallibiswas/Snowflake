{{ config(materialized='table') }}

with
-- ## CC1 = WiFi Walkby ##
-- ## CC2 = WiFi Walkins & POS + WiFi Walkin & POS + WiFi Walkby + POS ##
contact_profiles__ as (
    select client_macs, zenreach_contact, zenreach_fullname, pos_customer_id, pos_name, source, NULL business_id
    from {{ref('mart_merged_presence_pos__temporospatial_contact_profiles')}}
    union
    select NULL as client_macs, zenreach_contact, zenreach_fullname, pos_customer_id, pos_name, max(source), business_id
    from {{ref('mart_merged_presence_pos__crm_contact_profiles')}}
    group by 1,2,3,4,5,7
)
, contact_profiles_ as (
  select client_macs, zenreach_contact, zenreach_fullname, pos_customer_id, pos_name, source, business_id
  from contact_profiles__
)
, pos_and_wifi_sightings_ as (
  select
    s.*
    , c.pos_customer_id
    , c.source
    , t.pos_email
    , t.pos_name
    , t.pos_amount as amount
    , t.pos_purchase_id as purchase_id
    , t.pos_time_utc
    , t.pos_time
   from {{ref('mart_merged_presence_pos__enriched_sightings')}} s
   join contact_profiles_ c on s.contact = c.zenreach_contact and s.client_macs = c.client_macs
   join {{ref('mart_merged_presence_pos__transactions')}} t 
            on c.pos_customer_id = t.pos_customer_id 
            and s.business_id = t.business_id
            -- and t.pos_time_utc between dateadd(hours, -1*$temporal_buffer, s.start_time) and dateadd(hours, $temporal_buffer, s.end_time)
            and t.pos_time between dateadd(hours, -2, s.start_time) and dateadd(hours, 2, s.end_time)
   where s.start_time >= dateadd(days, -31, current_date()) -- all time '2019-01-01'
)
, pos_and_wifi_walkins_ as (
  select
    sighting_id
    , listagg(distinct purchase_id, ', ') within group (ORDER BY purchase_id) purchase_id
    , business_id
    , date_trunc(week, min(least(start_time_utc, pos_time_utc))) as week_of
    , min(least(start_time_utc, pos_time_utc)) as start_time
    , max(greatest(end_time_utc, pos_time_utc)) as end_time
    , contact as email_1
    , contact_alt_1 as email_2
    , contact_alt_2 as email_3
    , coalesce(get_first_name(pos_name), get_first_name(fullname), get_first_name(fullname_alt_1)) as first_name
    , coalesce(get_last_name(pos_name), get_last_name(fullname), get_last_name(fullname_alt_1)) as last_name
    , sum(s1.amount / 100) as amount
    , 'POS & WiFi' || ' ' || s1.classification as classification
    , 'CC3' as custom_conversion_type
   from pos_and_wifi_sightings_ s1
  group by sighting_id, business_id, email_1, email_2, email_3, first_name, last_name, classification, custom_conversion_type
)
, wifi_sightings_ as (
    select
      s.sighting_id
      , NULL as purchase_id
      , s.business_id
      , date_trunc(week, s.end_time_utc) as week_of
      , s.start_time_utc as start_time
      , s.end_time_utc as end_time
      , s.contact as email_1
      , s.contact_alt_1 as email_2
      , s.contact_alt_2 as email_3
      , coalesce(get_first_name(fullname), get_first_name(fullname_alt_1), get_first_name(fullname_alt_2)) as first_name
      , coalesce(get_last_name(fullname), get_last_name(fullname_alt_1), get_last_name(fullname_alt_2)) as last_name
      , iff(s.classification = 'WALKBY', .01, 10000) as amount
      , 'WiFi' || ' ' || s.classification as classification
      , iff(s.classification = 'WALKBY', 'CC1', 'CC2') as custom_conversion_type
   from {{ref('mart_merged_presence_pos__enriched_sightings')}} s
   where s.start_time >= dateadd(days, -31, current_date()) -- all time '2019-01-01'
         and s.sighting_id not in (select sighting_id from pos_and_wifi_walkins_ )
    order by end_time desc
)
-- ## CC3 - POS "Walkins" that do match to an identity but weren't aligned with a WiFi sighting
-- here we may match multiple identities, sourced via temporospatial, merchant or local CRM.
-- prioritize temporospatial, then merchant, then local CRM matches.
-- TODO: here the same email / name may appear as temporospatial matches 
-- TOOD: here we can probably do a little better pulling up alternative emails / names from the full contact_profile table
-- for temporospatial matches
-- TODO: This is only storing local time, utc is lost
, transactions_identified_ as (
  select 
        t.*
        , c.zenreach_contact
        , c.zenreach_fullname
        , c.source
        , row_number() over (partition by pos_purchase_id order by source desc, u.email_is_valid desc, u.email_score) as rank
        , u.email_is_valid
        , u.email_score
        , count(*) over (partition by pos_purchase_id) n
  from {{ref('mart_merged_presence_pos__transactions')}} t
  join contact_profiles_ c on t.pos_customer_id = c.pos_customer_id and t.business_id = c.business_id

  join {{ seed_or_ref( ref('src_crm__portal_userprofile'), 'seed_portal_userprofile') }} u on c.zenreach_contact = clean_contact(u.email)

  where  -- omit sightings already paired to POS
         t.pos_purchase_id not in (select purchase_id from pos_and_wifi_walkins_ )
         and t.pos_time >= dateadd(days, -31, current_date()) -- all time '2019-01-01'
)
, pos_identified_walkins_ as (
  select
    NULL as sighting_id
    , t1.pos_purchase_id as purchase_id
    , t1.business_id
    , date_trunc(week, t1.pos_time_utc) as week_of
    , NULL as start_time
    , t1.pos_time as end_time
    , t1.zenreach_contact as email_1
    , t2.zenreach_contact as email_2
    , coalesce(t1.pos_email, t3.zenreach_contact) as email_3
    , get_first_name(t1.pos_name) as first_name
    , get_last_name(t1.pos_name) as last_name
    , t1.pos_amount/100 as amount
    , 'POS WALKIN ' || UPPER(greatest(ifnull(t1.source,''), ifnull(t2.source,''), ifnull(t3.source,''))) as classification
    , 'CC4' as custom_conversion_type
  from transactions_identified_ t1
  left join transactions_identified_ t2 on t1.pos_purchase_id = t2.pos_purchase_id and t2.rank = 2
  left join transactions_identified_ t3 on t1.pos_purchase_id = t3.pos_purchase_id and t3.rank = 3
  where t1.rank = 1
)
, pos_unidentified_walkins_ as (
  select
    NULL as sighting_id
    , t.pos_purchase_id as purchase_id
    , t.business_id
    , date_trunc(week, t.pos_time_utc) as week_of
    , NULL as start_time
    , t.pos_time_utc as end_time
    , t.pos_email as email_1
    , NULL email_2
    , NULL as email_3
    , get_first_name(t.pos_name) as first_name
    , get_last_name(t.pos_name) as last_name
    , t.pos_amount/100 as amount
    , 'POS UNMATCHED WALKIN' || iff(t.pos_email is null, '', ' (WITH POS EMAIL)') as classification
    , 'CC5' as custom_conversion_type
  from {{ref('mart_merged_presence_pos__transactions')}} t
  where t.pos_purchase_id not in (select purchase_id from pos_and_wifi_walkins_ )
        and t.pos_purchase_id not in (select purchase_id from pos_identified_walkins_)
        and t.POS_PAYMENT_METHOD = 'CARD'
        and t.pos_name is not null
        and t.pos_time >= dateadd(days, -31, current_date()) -- all time '2019-01-01'
)
, merged_ as (
  select md5(sighting_id || purchase_id) as order_id, *, 'Purchase' as event_name
  from pos_and_wifi_walkins_
  union
  select md5(sighting_id) as order_id, *, 'Other' as event_name
  from wifi_sightings_
  union
  select md5(purchase_id) as order_id, *, 'Purchase' as event_name
  from pos_identified_walkins_
  union
  select md5(purchase_id) as order_id, *, 'Purchase' as event_name
  from pos_unidentified_walkins_
)
select 
    m.order_id
    , m.sighting_id
    , m.purchase_id
    , m.business_id
    , m.week_of
    , m.start_time
    , m.end_time
    , m.email_1
    , m.email_2
    , m.email_3
    , m.first_name
    , m.last_name
    , m.amount
    , m.custom_conversion_type
    , g.zip as postal_code
    , m.classification
    , current_timestamp() as created_at
    , convert_timezone('UTC', 'America/Los_Angeles', current_timestamp()) as created_at_pdt
from merged_ m

left join {{ seed_or_ref( ref('stg_business_profiles__d_business_geocode'), 'seed_d_business_geocode') }} g on m.business_id = g.business_id

order by end_time desc
