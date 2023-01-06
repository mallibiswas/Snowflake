--------------------------------------------------------------------------------------------
-- PRESENCE SAMPLING STATS
-- Replacement of ads_campaigns.presence_sampling_stats
-- Incremental load of presence.presence_sampling_stats from presence.finished_sightings
-- The table was created by a upstream Local Ads service and decommissioned on 10/29/2019
-- This table is used for calculating Recognition Rates and is thusly, being revived
-- as a daily summary table to be able to able to analyze variability of RRs for a business over time
--------------------------------------------------------------------------------------------

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{dbname};
use schema &{schemaname};
use role &{rolename};

SET MIN_END_TS = (select max(updated) from &{dbname}.&{schemaname}.PRESENCE_SAMPLING_STATS);

SET MAX_END_TS = (select date_trunc(hour,current_timestamp()));

-- log time stamp range
SELECT concat('Inserting from ts: $MIN_END_TS: ',$MIN_END_TS,' TO : $MAX_END_TS: ',$MAX_END_TS);

insert into &{dbname}.&{schemaname}.presence_sampling_stats (
            BUSINESS_ID,
            REPORT_DATETIME,
            WALKIN_NETWORK,
            WALKIN_MERCHANT,
            WALKIN_UNIDENTIFIED,
            WALKBY_NETWORK,
            WALKBY_MERCHANT,
            WALKBY_UNIDENTIFIED,
            not_human,
            CREATED,
            UPDATED,
            ASOF_DATE)
select    business_id
        , date_trunc(hour, start_time) as report_datetime
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
from    &{dbname}.&{schemaname}.finished_sightings
WHERE   end_time > $MIN_END_TS AND end_time < $MAX_END_TS
group by 1,2;
