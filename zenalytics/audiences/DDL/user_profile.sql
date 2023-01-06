---------------------------------------------------------------
---------- DDL for AUDIENCES.user_profile
---------------------------------------------------------------

create or replace table zenalytics.audiences.user_profile
-- One record per email (=customer_sk) Note: A single email can be associated with multiple people
-- customer_sk is a hash on the email address
as
select  customer_sk,
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
from zenalytics.crm.analytics_customer
group by customer_sk, email
order by updated;
