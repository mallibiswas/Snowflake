-----------------------------------------------------------------------
---------------------  ZENSHARE.MAIN.VISITS_SMRY ----------------------
-----------------------------------------------------------------------

use role &{rolename};
use warehouse &{whname};
use database &{targetdb};
use schema &{targetschema};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set timezone = 'UTC';

create or replace table &{targetdb}.&{targetschema}.visits_smry
as
select visit_date,
business_id,
contact_id,
contact_method,
visit_duration_mins,
visit_count,
first_visit,
visit_day,
first_start_time as create_dttm,
last_end_time as update_dttm,
current_date as asof_date
from  &{sourcedb}.&{sourceschema}.VISITS_SMRY_VW
;
