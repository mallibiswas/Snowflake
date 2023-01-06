/*
  Daily rebuild of reclassified_presence_sampling_stats table
  The logic for the table is in the reclassified_finished_sightings_vw
  The view will change as reclassifications evolve over time
*/

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{dbname};
use schema &{schemaname};
use role &{rolename};

create or replace table &{dbname}.&{schemaname}.reclassified_presence_sampling_stats
as
select    business_id
        , report_datetime
        , walkin_network
        , walkin_merchant
        , walkin_unidentified
        , walkby_network
        , walkby_merchant
        , walkby_unidentified
        , not_human -- New Classification since presence_sampling_stats was created
        , created
        , updated
        , asof_date
from    &{dbname}.&{schemaname}.reclassified_presence_sampling_stats_vw
order by 1,2;
