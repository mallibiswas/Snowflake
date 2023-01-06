use database zenalytics;
use role etl_prod_role;
use schema public;

create or replace function zenalytics.public.email_sample (bucket integer)
    returns table (email varchar)
    as
    $$
        select email from zenalytics.public.email_master_list_100days sample (20000 rows) where email_score in (bucket)
    $$
    ;

grant usage on function zenalytics.public.email_sample (integer) to read_only_role;
grant usage on function zenalytics.public.email_sample (integer) to read_zenalytics_role;

create or replace temporary table _clean_email_master_list_
as
  select    up.email,
            up.userprofile_id,
            up.email_is_valid,
            date_added,
            10*trunc(email_score,1) as email_score, -- create bucket
            ac.gender,
            ac.income,
            bounced,
            recipient_domain
  from  zenalytics.crm.portal_userprofile up,
        zenalytics.crm.analytics_customer ac,
        zenalytics.crm.smbsite_messagelog ml
  where ac.email = up.email and ml.userprofile_id = up.userprofile_id
  and   date_added > dateadd(day,-120,current_date()) -- emails created in the last 100 days
  and   up.email_is_valid = True and ac.email_is_valid = True -- valid email
  and   up.email not like '%@qos.zenreach.com' -- not system email
  and   REGEXP_LIKE (up.EMAIL, '^[A-Za-z]+[A-Za-z0-9.]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$') -- valid structure
  and   NVL(ac.non_customer,False) = False -- not employee
  and   NVL(ac.non_employee,False) = False -- not employee
--  and ac.gender is null and ac.income is null -- no demo info
  and   bounced is null;  -- not bounced

create or replace table zenalytics.public.email_master_list_100days
as
select distinct email, userprofile_id, email_score
from _clean_email_master_list_;


create or replace table zenalytics.public.match_test_email_list
as
select *, '6' as bucket from table(zenalytics.public.email_sample (6))
UNION
select *, '7' as bucket from table(zenalytics.public.email_sample (7))
UNION
select *, '8' as bucket from table(zenalytics.public.email_sample (8))
UNION
select *, '9' as bucket from table(zenalytics.public.email_sample (9))
UNION
select *, '10' as bucket from table(zenalytics.public.email_sample (10))

----------------------------------------------------------
--- check for duplicates
----------------------------------------------------------

select email, count(*)
from
    (select *, '6' as bucket from table(zenalytics.public.email_sample (6))
    UNION ALL
    select *, '7' as bucket from table(zenalytics.public.email_sample (7))
    UNION ALL
    select *, '8' as bucket from table(zenalytics.public.email_sample (8))
    UNION ALL
    select *, '9' as bucket from table(zenalytics.public.email_sample (9))
    UNION ALL
    select *, '10' as bucket from table(zenalytics.public.email_sample (10))
    )
group by 1
having count(*) > 1;

select email, count(*)
from zenalytics.public.match_test_email_list
group by 1
having count(*) > 1;

----------------------------------------------------------
--- check totals
----------------------------------------------------------

select count(*)
from
    (select *, '6' as bucket from table(zenalytics.public.email_sample (6))
    UNION ALL
    select *, '7' as bucket from table(zenalytics.public.email_sample (7))
    UNION ALL
    select *, '8' as bucket from table(zenalytics.public.email_sample (8))
    UNION ALL
    select *, '9' as bucket from table(zenalytics.public.email_sample (9))
    UNION ALL
    select *, '10' as bucket from table(zenalytics.public.email_sample (10))
    );

select count(*), count(distinct email)
from zenalytics.public.match_test_email_list;
