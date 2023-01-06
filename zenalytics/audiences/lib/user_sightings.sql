---------------------------------------------------------------
---------- Upsert for AUDIENCES.user_sightings
---------------------------------------------------------------

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{tgtdbname};
use schema &{tgtschemaname};
use role &{rolename};

SET MIN_END_TS = (select max(created) from &{tgtdbname}.&{tgtschemaname}.user_sightings);

SET MAX_END_TS = (select dateadd(day,-1,to_timestamp_ntz(current_date())));

-- log time stamp range
SELECT concat('Inserting from ts: $MIN_END_TS: ',$MIN_END_TS,' TO : $MAX_END_TS: ',$MAX_END_TS);

-- create temp table to hold (business_id, email) combinations from all sightings (walins with valid non-employee emails)
create or replace temporary table user_sightings_
as
select  business_id,
        contact_id,
        substr(sha1(contact_info),1,24) as customer_sk,
        MIN(start_time) as first_sighted,
        MAX(end_time) as last_sighted,
        count(distinct sighting_id) as sightings
from &{srcdbname}.PRESENCE.FINISHED_SIGHTINGS
where classification = 'WALKIN'
and contact_id is not null
and contact_info like '%@%'
and ifnull(is_employee, FALSE) = FALSE
and end_time > to_timestamp_ntz($MIN_END_TS) and end_time <= to_timestamp_ntz($MAX_END_TS)
group by business_id, contact_id, contact_info;

MERGE into &{tgtdbname}.&{tgtschemaname}.user_sightings tgt
  using user_sightings_ src
  on tgt.business_id = src.business_id and tgt.contact_id = src.contact_id
  when matched and tgt.last_sighted < src.first_sighted
    then update
      set tgt.last_sighted = src.last_sighted,
          tgt.sightings = tgt.sightings + src.sightings,
          tgt.updated = src.last_sighted,
          target.asof_date = current_date()
  when not matched
    then insert (business_id, contact_id, customer_sk, first_sighted, last_sighted, created, updated, sightings, asof_date)
    -- for brand new records created and updated are same as first and last sighted
    values (src.business_id, src.contact_id, src.customer_sk, src.first_sighted, src.last_sighted, src.first_sighted, src.last_sighted, src.sightings, current_date());
