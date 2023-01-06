/*
  Daily rebuild of reclassified_presence_sampling_stats_demographics table
  This tables does not have the same data corrections as reclassified_presence_sampling_stats table
*/

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{dbname};
use schema &{schemaname};
use role &{rolename};

create or replace table &{dbname}.&{schemaname}.reclassified_presence_sampling_stats_demographics
as
select s.business_id
        , date_trunc(hour, start_time) as report_datetime
        , ages[0]::string as age
        , genders[0]::string as gender
        , incomes[0]::string as income
        , SUM(iff(classification = 'WALKIN' AND known_to_zenreach AND NOT(known_to_merchant_account) AND NOT(known_to_merchant_location), 1, 0)) as walkin_network
        , SUM(iff(classification = 'WALKIN' AND (known_to_merchant_account OR known_to_merchant_location), 1, 0)) as walkin_merchant
        , SUM(iff(classification = 'WALKIN' AND not(known_to_zenreach), 1, 0)) as walkin_unidentified
        , SUM(iff(classification = 'WALKBY' AND known_to_zenreach AND NOT(known_to_merchant_account) AND NOT(known_to_merchant_location), 1, 0)) as walkby_network
        , SUM(iff(classification = 'WALKBY' AND (known_to_merchant_account OR known_to_merchant_location), 1, 0)) as walkby_merchant
        , SUM(iff(classification = 'WALKBY' AND NOT(known_to_zenreach), 1, 0)) as walkby_unidentified
        , SUM(iff(classification = 'NOTHUMAN', 1, 0)) as not_human -- New Classification since presence_sampling_stats was created
        , MIN(start_time) as created
        , MAX(end_time) as updated
        , current_date as asof_date
from    &{dbname}.&{schemaname}.reclassified_finished_sightings_vw s
left join &{dbname}.&{srcschemaname}.user_profile p
on p.customer_sk = s.customer_sk
group by 1,2,3,4,5
order by 1,2;
