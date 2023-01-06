select customer_id,
       business_id,
       phone,
       case
           when age in ('13-17', '18-20', '21-24', '25-34', '35-44', '45-54', '55-64', '65+') then age
           when age = '' or age = 'None' or age is null then null
           when left(age, 2) <= 17 then '13-17'
           when left(age, 2) <= 20 then '18-20'
           when left(age, 2) <= 24 then '21-24'
           when left(age, 2) <= 34 then '25-34'
           when left(age, 2) <= 44 then '35-44'
           when left(age, 2) <= 54 then '45-54'
           when left(age, 2) <= 64 then '55-64'
           when left(age, 2) >= 65 then '65+'
           else 'other'
           end                                                        as age,
       city,
       contact_allowed,
       default_pic_url,
       email,
       substr(sha1(email), 1, 24)                                     as customer_sk,
       email_is_valid,
       emails,
       fullname,
       case
           when gender is null or gender = '' or gender = 'None' then null
           when lower(gender) = 'f' or lower(gender) like 'fe%' or lower(gender) = 'mujer' or lower(gender) = 'weiblich'
               then 'Female'
           when lower(gender) = 'm' or lower(gender) = 'masculino' or lower(gender) like 'male%' or
                lower(gender) = 'hombre' then 'Male'
           else 'Other' end                                           as gender,
       case
           when income is null or income = '' or income = 'None' then null
           else income end                                            as income,
       case when lower(tags) like '%wifi%' then 'WIFI' else 'CRM' end as customer_type,
       created,
       first_seen,
       last_seen,
       last_updated,
       server_last_seen,
       location,
       state,
       zip_code,
       tags,
       macs,
       facebook_app_user_ids,
       messages_sent,
       birthday_day,
       birthday_month,
       non_customer,
       non_employee,
       offers_redeemed,
       offers_sent,
       purchase_count,
       validation_reason,
       visit_count,
       asof_date
from {{ ref('src_crm__analytics_customer') }}
