-- QUERY 1 - PULL OUT WIFI FINISHED SIGHTINGS -> 2880 rows
CREATE OR REPLACE TABLE zenprod.presence.wifi_finished_sightings_2020_04_15_omit_2
as
with deduped as (
select distinct *
FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS AS s
WHERE s.end_time > '2020-04-15T15:00:00'
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
  WHERE s.end_time > '2020-04-15T15:00:00'
  AND s.end_time < '2020-04-16T10:00:00'
  AND NOT startswith(s.client_mac_info[0].client_mac, '123') // filter out QoS
)
select
    *
from sightings_
where ifnull(sighting_lag, 1000000) < 60;

-- QUERY 2 - PULL OUT WIFI CONSENTED SIGHTINGS
CREATE OR REPLACE TABLE zenprod.presence.wifi_consented_sightings_2020_04_15_omit_2
as
with deduped as (
    select distinct *
    FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS AS s
    WHERE s.end_time > '2020-04-15T17:00:00'
)
select s.*
from deduped AS s
join zenprod.presence.wifi_finished_sightings_2020_04_15_omit_2 as o on s.sighting_id = o.sighting_id


-- QUERY 3 - DELETE FROM WIFI FINISHED SIGHTINGS
DELETE FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS s
USING zenprod.presence.wifi_finished_sightings_2020_04_15_omit_2 o where s.sighting_id = o.sighting_id;


-- QUERY 4 - DELETE FROM WIFI CONSENTED SIGHTINGS
DELETE FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS s
USING zenprod.presence.wifi_consented_sightings_2020_04_15_omit_2 o where s.sighting_id = o.sighting_id;