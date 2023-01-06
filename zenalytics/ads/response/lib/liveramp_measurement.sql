---------------------------------------------------------------
--------------------- LIVERAMP_MEASUREMENT --------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

create or replace transient table &{dbname}.&{schemaname}.uploaded_sightings_liveramp
as
    select
    $1 as uploaded_sightings_id,
    $2 as sighting_id,
    $3 as business_id,
    $4::timestamp as end_time,
current_timestamp as asof_date
FROM @&{stagename}/&{stagepath}/uploaded_sightings.csv
;
