{{
    config(
        materialized='incremental',
        unique_key='customer_sk',
        incremental_strategy ='delete+insert'

    )
}}


{% if is_incremental() %}
with recent_users_ as (
    SELECT distinct customer_sk
    FROM {{ ref('stg_crm__analytics_customer') }}
    WHERE created>(select(dateadd(day,-3,to_timestamp_ntz(max(created))))from {{ this }})
)
{% endif %}

SELECT  
    customer_sk,
    email,
    MIN(created) as created,
    MAX(created) as updated,
    arrayagg(distinct emails) as emails,
    arrayagg(distinct age) as ages,
    arrayagg(distinct city) as cities,
    arrayagg(distinct gender) as genders,
    arrayagg(distinct income) as incomes,
    -- create a payload of (business id, customer id, customer type) this will be useful when we get other sources like POS
    arrayagg(
        parse_json('{"business_id":"'||business_id||'",'||'"customer_id":"'||customer_id||'",'||'"customer_type":"'||customer_type||'",'||'"non_employee":"'||NVL(non_employee,True)||'"}'::variant)
        ) as business_tags
FROM {{ ref('stg_crm__analytics_customer') }}
{% if is_incremental() %}
       -- this filter will only be applied on an incremental run
    WHERE customer_sk in (select customer_sk from recent_users_ )
{% endif %}
group by customer_sk, email
order by customer_sk
