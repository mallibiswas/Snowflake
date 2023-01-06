-----------------------------------------------------------------------
---------------------  ZENALYTICS.PRESENCE.VISITS_SMRY ----------------------
-----------------------------------------------------------------------

use role &{rolename};
use warehouse &{whname};
use database &{tgtdbname};
use schema &{tgtschemaname};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set timezone = 'UTC';

create or replace table &{tgtdbname}.&{tgtschemaname}.visits_smry
as
select v.*,
current_date() as asof_date
from  &{tgtdbname}.&{tgtschemaname}.VISITS_SMRY_VW v
;
