-- These scripts were created to resolve an issue on Apr 15 2020 where sightings split due to lag in the blip section of the presence pipeline.
-- The goal of these scripts is to find all the sightings that were split and delete all but the first in the chain for that sighting.
-- eg. If a sighting was split into 12:00-12:30, 12:31-12:32, 12:35-12:40, then only the first one (12:00-12:30) would be kept.

-- If we ever have to replay sightings during this time period, the split sightings could reappear. In that case running the delete queries
-- (query 3 and 4) should remove them again.



-- QUERY 1 - PULL OUT WIFI FINISHED SIGHTINGS -> 35376 rows
CREATE OR REPLACE TABLE zenprod.presence.wifi_finished_sightings_2020_04_15_omit
as
with deduped as (
select distinct *
FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS AS s
WHERE s.end_time > '2020-04-15T17:00:00'
AND s.end_time < '2020-04-16T10:00:00'
)
, sightings_ as (
  SELECT 
    DATEDIFF(
            milliseconds
            , lag(s.end_time) OVER (PARTITION BY location_id, coalesce(contact_id, client_mac_info[0]:client_mac) ORDER BY start_time)
            , s.start_time)/60000 AS sighting_lag
    , s.*
  FROM deduped AS s
  WHERE s.end_time > '2020-04-15T17:00:00'
  AND s.end_time < '2020-04-16T10:00:00'
  AND NOT startswith(s.client_mac_info[0].client_mac, '123') // filter out QoS
)
select
    *
from sightings_
where ifnull(sighting_lag, 1000000) < 60;


-- QUERY 2 - PULL OUT WIFI CONSENTED SIGHTINGS
CREATE OR REPLACE TABLE zenprod.presence.wifi_consented_sightings_2020_04_15_omit
as
with deduped as (
    select distinct *
    FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS AS s
    WHERE s.end_time > '2020-04-15T17:00:00'
)
select s.*
from deduped AS s
join zenprod.presence.wifi_finished_sightings_2020_04_15_omit as o on s.sighting_id = o.sighting_id



-- QUERY 3 - DELETE FROM WIFI FINISHED SIGHTINGS
DELETE FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS s
USING zenprod.presence.wifi_finished_sightings_2020_04_15_omit o where s.sighting_id = o.sighting_id;



-- QUERY 4 - DELETE FROM WIFI CONSENTED SIGHTINGS
DELETE FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS s
USING zenprod.presence.wifi_consented_sightings_2020_04_15_omit o where s.sighting_id = o.sighting_id;