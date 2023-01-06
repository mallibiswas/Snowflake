
alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

-----------------------------------------------------------------------
--------------------- last portal blip       --------------------------
-----------------------------------------------------------------------

use warehouse &{whname};
use role &{rolename};
-- use schema &{schemaname};

merge into &{dbname}.&{schemaname}.Down_Router_Last_Portal_Blip target
using (SELECT b.SENSOR_MAC, 
     MAX(TO_TIMESTAMP(b.SERVER_TIME)) AS LAST_TS
     FROM &{dbname}.presence.portal_blips b
     where TO_TIMESTAMP(b.SERVER_TIME) between to_date('&{period_begin_date}') and to_date('&{period_end_date}')
     GROUP BY b.SENSOR_MAC) src on target.SENSOR_MAC = src.SENSOR_MAC
when matched and src.LAST_TS > target.LAST_TS then update set target.LAST_TS = src.LAST_TS, target.ASOF_DATE = to_date('&{ASOF_DATE}')
when not matched then insert (SENSOR_MAC, LAST_TS, ASOF_DATE) values (src.SENSOR_MAC, src.LAST_TS, to_date('&{ASOF_DATE}'));

