-- We filter on all Peets locations
-- And we use the following time ranges to update data
    -- 2021-01-14 19:06:00.000000000 --> First minute jumping from <5 sightings per minute to 56,
    -- 2021-01-14 21:47:00.000000000 --> Last minute jumping from >11 sightings back to <5 per minute
    --OR BETWEEN
    --     2021-01-15 13:46:00.000000000 --> First minute jump from avg 50 sightinhgs to 100+
    --     2021-01-15 20:56:00.000000000 --> Last minute jump from 300+ to about avg 50 again.
-- We noticed an uptick in blips with the count of 2 for this filtering
-- if we change these from CLASSIFICATION = 'Classification_WALKIN' to 'Classification_WALKBY' the numbers return to a more normal curve

BEGIN TRANSACTION;

CREATE TABLE ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18
COMMENT = 'This table has been created to backup peets data with the following filter:
WHERE (
        (START_TIME >= ''2021-01-14 19:06:00''
         AND START_TIME <= ''2021-01-14 21:47:00'')
     OR
        (START_TIME >= ''2021-01-15 13:46:00''
         AND START_TIME <= ''2021-01-15 20:56:00'')
  )
  AND lower(LOCATION_ID) IN
      (SELECT lower(BUSINESS_ID)
       FROM ZENPROD.CRM.BUSINESSPROFILE_HIERARCHY
       WHERE L1_ID = ''591c90aa4a4f9f000c17a55e'')
  AND BLIP_COUNT = 2;
After the backup the data has been updated using this logic where CLASSIFICATION = ''Classification_WALKIN'' to ''Classification_WALKBY''
This was needed due to an update to Meraki V3 API which created wrong data, with this update the numbers return to a more normal curve' AS
SELECT *
FROM zenprod.presence.wifi_finished_sightings
WHERE (
        (START_TIME >= '2021-01-14 19:06:00'
         AND START_TIME <= '2021-01-14 21:47:00')
     OR
        (START_TIME >= '2021-01-15 13:46:00'
         AND START_TIME <= '2021-01-15 20:56:00')
  )
  AND lower(LOCATION_ID) IN
      (SELECT lower(BUSINESS_ID)
       FROM ZENPROD.CRM.BUSINESSPROFILE_HIERARCHY
       WHERE L1_ID = '591c90aa4a4f9f000c17a55e')
  AND BLIP_COUNT = 2

UNION ALL

SELECT *
FROM zenprod.presence.wifi_finished_sightings
WHERE (END_TIME >= '2021-01-15 20:50:00'
         AND END_TIME <= '2021-01-15 20:59:00')
  AND lower(LOCATION_ID) IN
      (SELECT lower(BUSINESS_ID)
       FROM ZENPROD.CRM.BUSINESSPROFILE_HIERARCHY
       WHERE L1_ID = '591c90aa4a4f9f000c17a55e')
  AND BLIP_COUNT > 5;

-- SELECT COUNT(*)
-- FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18;

-- SELECT COUNT(*)
-- FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18
-- WHERE CLASSIFICATION = 'Classification_WALKIN';

-- SELECT *
-- FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18;

CREATE TABLE ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS__BACKUP_PEETS_2021_01_18
COMMENT = 'This table has been created to backup peets data with the following filter:
WHERE (
        (START_TIME >= ''2021-01-14 19:06:00''
         AND START_TIME <= ''2021-01-14 21:47:00'')
     OR
        (START_TIME >= ''2021-01-15 13:46:00''
         AND START_TIME <= ''2021-01-15 20:56:00'')
  )
  AND lower(LOCATION_ID) IN
      (SELECT lower(BUSINESS_ID)
       FROM ZENPROD.CRM.BUSINESSPROFILE_HIERARCHY
       WHERE L1_ID = ''591c90aa4a4f9f000c17a55e'')
  AND BLIP_COUNT = 2;
After the backup the data has been updated using this logic where CLASSIFICATION = ''Classification_WALKIN'' to ''Classification_WALKBY''
This was needed due to an update to Meraki V3 API which created wrong data, with this update the numbers return to a more normal curve' AS
SELECT *
FROM zenprod.presence.WIFI_CONSENTED_SIGHTINGS
WHERE SIGHTING_ID IN (
    SELECT SIGHTING_ID FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18
    );

UPDATE ZENPROD.PRESENCE.wifi_finished_sightings --> 28358 rows affected in 2 m 46 s 602 ms
SET CLASSIFICATION = 'Classification_WALKBY'
WHERE SIGHTING_ID IN (
    SELECT SIGHTING_ID FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18
    )
    AND CLASSIFICATION = 'Classification_WALKIN';

UPDATE ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS
SET CLASSIFICATION = 'Classification_WALKBY'
WHERE SIGHTING_ID IN (
    SELECT SIGHTING_ID FROM ZENPROD.PRESENCE.WIFI_FINISHED_SIGHTINGS__BACKUP_PEETS_2021_01_18
    )
    AND CLASSIFICATION = 'Classification_WALKIN';

--COMMIT;