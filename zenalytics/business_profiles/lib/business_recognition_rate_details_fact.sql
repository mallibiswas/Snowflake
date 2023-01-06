-------------------------------------------------------------------
---------- Recognition Rates Table
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{targetschemaname};

create or replace table &{dbname}.&{targetschemaname}.business_recognition_rates_details_fact
as
WITH valid_walkins as (select business_id,
                       date_trunc(month,report_datetime)::date as report_month,
                       walkin_merchant+walkin_network as recognized_walkins,
                       walkin_merchant+walkin_network+walkin_unidentified as total_walkins
                       from &{dbname}.&{sourceschemaname}.presence_sampling_stats
                       where  report_datetime < date_trunc(month,current_date)
                       and report_datetime >= '2019-12-01'
                      )
select business_id,
       report_month,
       SUM(recognized_walkins) as recognized_walkins,
       SUM(total_walkins) as total_walkins,
       current_date as asof_date
from   valid_walkins
where total_walkins > 100
and recognized_walkins>0.01*total_walkins
group by 1,2;
