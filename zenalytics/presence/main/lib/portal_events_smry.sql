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

create or replace table &{dbname}.&{schemaname}.portal_events_smry
as
select  date_trunc(day,created) as event_date,
        location_id as business_id,
        count(id) as authentications,
        current_date as asof_date
from &{dbname}.&{schemaname}.portal_events
where event_type='passed authorization'
group by 1,2;
