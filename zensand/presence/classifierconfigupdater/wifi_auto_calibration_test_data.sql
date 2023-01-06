-- ----------------- ----------------- -----------------
-- !This file has only SQL to be used for test purposes!
-- To test the task WIFI_CALCULATE_SIGNAL_STRENGTH_TASK follow these steps:
-- 1. Create the table WIFI_CONSENTED_SIGHTINGS_TEST_DATA using the statement below.
-- 2. Change the task use `WIFI_CONSENTED_SIGHTINGS_TEST_DATA` instead of `WIFI_CONSENTED_SIGHTINGS`
-- 3. Comment out the condition `(name NOT ILIKE '%qos%')` from the task
-- 4. Make the "JOIN zensand.crm.portal_businessprofile" a LEFT JOIN
-- 5. Update the task OR run only the "MERGE INTO" part of it.
-- 6. You may also repeat the INSERT statement below at least 6 times to create locations with at leas 60 entries
--
-- After running the "MERGE INTO" or after the task has ran at least one, check the table `WIFI_AUTO_SIGNAL_STRENGTH_CALIBRATION`
--
-- !!Please wipe out the fake data you may have created at WIFI_AUTO_SIGNAL_STRENGTH_CALIBRATION
-- ----------------- ----------------- -----------------

create or replace TABLE ZENSAND.PRESENCE.WIFI_CONSENTED_SIGHTINGS_TEST_DATA (
	SIGHTING_ID VARCHAR(16777216),
	CLASSIFICATION VARCHAR(16777216),
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	BLIP_COUNT NUMBER(38,0),
	MAX_RSSI NUMBER(38,0),
	MIN_RSSI NUMBER(38,0),
	AVG_RSSI FLOAT,
	ANONYMOUS_CLIENT_MAC_INFO ARRAY,
	CONTACT_ID VARCHAR(16777216),
	CONTACT_INFO VARCHAR(16777216),
	CONTACT_METHOD VARCHAR(16777216),
	LOCATION_ID VARCHAR(16777216),
	ACCOUNT_ID VARCHAR(16777216),
	KNOWN_TO_ZENREACH BOOLEAN,
	KNOWN_TO_MERCHANT_ACCOUNT BOOLEAN,
	KNOWN_TO_MERCHANT_LOCATION BOOLEAN,
	PRIVACY_VERSION VARCHAR(16777216),
	TERMS_VERSION VARCHAR(16777216),
	BUNDLE_VERSION VARCHAR(16777216),
	IS_EMPLOYEE BOOLEAN,
	PORTAL_BLIP_COUNT NUMBER(38,0)
);


-- run this block at least 6 time to test the normal conditions
INSERT INTO ZENSAND.PRESENCE.WIFI_CONSENTED_SIGHTINGS_TEST_DATA (
location_id,
max_rssi,
blip_count,
portal_blip_count,
start_time,
end_time)
VALUES
-- execute this block only once to get the 'low_n_sightings' alert
('low_sightings', -78, 70, 84, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -80, 0, 12, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -65, 45, 0, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -81, 21, 20, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -77, 0, 30, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -78, 52, 0, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -67,  200, 658, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -89, 0, 54, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -76, 40, 0, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('low_sightings', -77, 40, 54, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),

('normal', -78, 70, 84, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -80, 0, 12, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -65, 45, 0, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -81, 21, 20, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -77, 0, 30, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -78, 52, 41, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -67,  200, 0, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -89, 40, 54, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -76, 0, 54, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),
('normal', -77, 40, 0, '2020-07-05 15:00:31.000', '2020-07-05 15:40:31.000'),

('no-portal-blips', -78, 84, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -80, 12, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -65, 40, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -81, 20, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -77, 30, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -78, 41, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -67, 658, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -89, 54, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -68, 54, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('no-portal-blips', -77, 54, 0, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),

('high-rssi', -40, 84, 70, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -44, 12, 15, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -35, 40, 45, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -38, 20, 21, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -35, 30, 44, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -40, 41, 52, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -41, 658, 200, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -20, 54, 40, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -28, 54, 40, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('high-rssi', -34, 54, 40, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),

('low-rssi', -98, 84, 70, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -102, 12, 15, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -95, 40, 45, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -110, 20, 21, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -108, 30, 44, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -102, 41, 52, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -109, 658, 200, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -100, 54, 40, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -106, 54, 40, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000'),
('low-rssi', -107, 54, 40, '2020-07-05 14:00:31.000', '2020-07-05 15:40:31.000')

;


