---------------------------------------------------------------
---------- Upsert for AUDIENCES.user_profile
---------------------------------------------------------------

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{tgtdbname};
use schema &{tgtschemaname};
use role &{rolename};

SET MIN_END_TS = (select max(created) from &{tgtdbname}.&{tgtschemaname}.user_profile);

SET MAX_END_TS = (select dateadd(day,-1,to_timestamp_ntz(current_date())));

-- log time stamp range
SELECT concat('Inserting from ts: $MIN_END_TS: ',$MIN_END_TS,' TO : $MAX_END_TS: ',$MAX_END_TS);

create or replace temporary table recent_users_
as
select distinct customer_sk
from &{srcdbname}.&{srcschemaname}.analytics_customer
where created >= dateadd(day,-1,to_timestamp_ntz($MIN_END_TS));

delete from &{tgtdbname}.&{tgtschemaname}.user_profile
where customer_sk in (select customer_sk from recent_users_);

create or replace temporary table user_profile_
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
from &{srcdbname}.&{srcschemaname}.analytics_customer
where customer_sk in (select customer_sk from recent_users_)
group by customer_sk, email
order by customer_sk;

insert into &{tgtdbname}.&{tgtschemaname}.user_profile (customer_sk, email, created, updated, emails, ages, cities, genders, incomes)
select customer_sk, email, created, updated, emails, ages, cities, genders, incomes
from user_profile_;
