---------------------------------------------------------------
---------- DDL for AUDIENCES.user_sightings
---------------------------------------------------------------

-- create temp table to hold (business_id, email) combinations from all sightings (walins with valid non-employee emails)
create or replace temporary table zendev.audiences.user_sightings_
as
select  business_id,
        contact_id,
        substr(sha1(contact_info),1,24) as customer_sk,
        start_time,
        end_time
from ZENALYTICS.PRESENCE.FINISHED_SIGHTINGS
where classification = 'WALKIN'
and contact_id is not null
and contact_info like '%@%'
and ifnull(is_employee, FALSE) = FALSE;

create or replace table zenalytics.audiences.user_sightings
as
select  business_id,
        contact_id,
        customer_sk,
        max(start_time) as created,
        max(end_time) as updated,
        created as first_sighted,
        updated as last_sighted,
        count(*) as sightings
from zenalytics.audiences.user_sightings_
group by business_id, contact_id, customer_sk
order by created;
