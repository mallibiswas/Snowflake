
alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

-----------------------------------------------------------------------
--------------------- down router blip snapshot -----------------------
-----------------------------------------------------------------------

use warehouse &{whname};
use database &{dbname};
use role &{rolename};
--use schema &{schemaname};

MERGE INTO &{dbname}.&{schemaname}.down_router_blip_snapshot target
USING (
WITH cte_portal_blip as 
(SELECT b.SENSOR_MAC, 
     	MAX(TO_TIMESTAMP(b.SERVER_TIME)) AS LAST_TS,
     	c.report_date_sk, 
	c.report_date
     FROM &{dbname}.PRESENCE.PORTAL_BLIPS b
     INNER JOIN &{dbname}.BIZINT.D_DATE c
        ON c.report_date = TO_DATE(TO_TIMESTAMP(b.SERVER_TIME))
     where c.report_date = to_date('&{ASOF_DATE}')
     GROUP BY c.report_date_sk, c.report_date, b.SENSOR_MAC
),
cte_location_blip as 
(SELECT b.SENSOR_MAC, 
     	MAX(TO_TIMESTAMP(b.SERVER_TIME)) AS LAST_TS,
      	c.report_date_sk, 
	c.report_date
     FROM &{dbname}.PRESENCE.LOCATION_BLIPS b
     INNER JOIN &{dbname}.BIZINT.D_DATE c
        ON c.report_date = TO_DATE(TO_TIMESTAMP(b.SERVER_TIME))
     where c.report_date = to_date('&{ASOF_DATE}')
     GROUP BY c.report_date_sk, c.report_date, b.SENSOR_MAC
)
     SELECT l.SENSOR_MAC as location_sensor_mac, 
     p.sensor_mac as portal_sensor_mac,
     p.LAST_TS AS LAST_PORTAL_BLIP_TS,
     l.LAST_TS AS LAST_LOCATION_BLIP_TS,
     NVL(l.report_date,p.report_date) as asof_date
     FROM cte_portal_blip p
     FULL OUTER JOIN cte_location_blip l
        ON p.report_date_sk = l.report_date_sk
        AND p.sensor_mac = l.sensor_mac
) src on target.asof_date = src.asof_date 
WHEN NOT matched THEN INSERT (location_sensor_mac, portal_sensor_mac, LAST_PORTAL_BLIP_TS, LAST_LOCATION_BLIP_TS, ASOF_DATE) 
VALUES (src.location_sensor_mac, src.portal_sensor_mac, src.LAST_PORTAL_BLIP_TS, src.LAST_LOCATION_BLIP_TS, to_date('&{ASOF_DATE}'));


