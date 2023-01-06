-----------------------------------------------------------------------
--------------------- AUDIENCE_VISIT_AGG     --------------------------
-----------------------------------------------------------------------

use role &{rolename};
use database &{targetdbname};
use warehouse &{whname};
use schema &{targetschemaname};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

MERGE into &{targetdbname}.&{targetschemaname}.AUDIENCE_VISIT_AGG target
  using
    (
      SELECT location_id as business_id
      , contact_info as email
      , count(*) as visit_count
      , min(start_time) as first_seen
      , max(end_time) as last_seen
      FROM &{sourcedbname}.&{sourceschemaname}.WIFI_CONSENTED_SIGHTINGS
      WHERE classification = 'Classification_WALKIN' and contact_method = 'CONTACT_METHOD_EMAIL' AND contact_info IS NOT NULL
      and is_employee = false
      and end_time >= to_timestamp_ntz(dateadd(hours, -24, current_timestamp())) -- lookback 24 hours
      GROUP BY (location_id, contact_info)
    )  src
  on target.business_id = src.business_id and target.email = src.email
  when matched and target.last_seen < src.first_seen
  then update
    set target.last_seen = src.last_seen, target.visit_count = target.visit_count + src.visit_count, target.asof_date = current_date()
  when not matched
  then insert (business_id, email, visit_count, first_seen, last_seen, asof_date)
  values (src.business_id, src.email, src.visit_count, src.first_seen, src.last_seen, current_date());
