--------------------------------------------------------------------
-------------- Wifi Auto Signal Strength Calibration
--------------------------------------------------------------------

create table if not exists ZENPROD.PRESENCE.WIFI_AUTO_SIGNAL_STRENGTH_CALIBRATION (
    LOCATION_ID VARCHAR(16777216) NOT NULL,
    SIGNAL_STRENGTH INTEGER NOT NULL,
    NEEDS_MANUAL_INTERVENTION BOOLEAN NOT NULL,
    MANUAL_INTERVENTION_REASON VARCHAR(16777216),
    PORTAL_ONLY_SIGHTINGS INTEGER NOT NULL,
    LOCATION_ONLY_SIGHTINGS INTEGER NOT NULL,
    PORTAL_AND_LOCATION_SIGHTINGS INTEGER NOT NULL,
    CREATED_AT TIMESTAMP NOT NULL,
    UPDATED_AT TIMESTAMP NOT NULL
);

create or replace task ZENPROD.PRESENCE.WIFI_CALCULATE_SIGNAL_STRENGTH_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 0 * * 1 America/Los_Angeles' -- At 00:00 on Monday in PDT tz
AS
    MERGE INTO ZENPROD.PRESENCE.WIFI_AUTO_SIGNAL_STRENGTH_CALIBRATION as calibration USING (
        WITH rssi_percentiles_ AS (
            SELECT
                LOWER(location_id) AS location_id
                , max_rssi
                , percent_rank() over (partition BY LOWER(location_id) ORDER BY max_rssi) AS perc_rssi
            FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS
            WHERE
                datediff(minutes, start_time, end_time)/60 < 12 -- don't include very loooong sightings
                AND start_time >= dateadd(day, -30, CURRENT_DATE())
                AND blip_count > 0 -- there must be at least some location blips
                AND portal_blip_count > 0 -- portal blips included
        )

        , rssi_cutoffs_ AS (
            SELECT
                location_id
                , greatest(least(MAX(CASE WHEN perc_rssi < 0.105 THEN max_rssi ELSE NULL END), -65), -85) AS rssi_cutoff_thresholded

                -- Troubleshooting raw values
                , MAX(CASE WHEN perc_rssi < 0.105 THEN max_rssi ELSE NULL END) AS rssi_cutoff
                , COUNT(*) AS n_sightings

                -- validity checks --> warnings & alerts
                , COUNT(*) < 60 AS checks_n_sightings
                , MAX(CASE WHEN perc_rssi < 0.105 THEN max_rssi ELSE NULL END) > -45 AS checks_high_suggested_threshold
                , MAX(CASE WHEN perc_rssi < 0.105 THEN max_rssi ELSE NULL END) < -88 AS checks_low_suggested_threshold
            FROM rssi_percentiles_
            GROUP BY location_id
        )

        , previous_config_ AS (
            SELECT
                l.location_id
                , l.min_walkin_signal_strength AS set_signal_strength
            FROM ZENPROD.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS l
            JOIN (
                SELECT
                    location_id
                    , MAX(effective_as_of) AS last_set
                FROM ZENPROD.PRESENCE.LOCATION_CLASSIFIER_CONFIG_THRESHOLDS
                GROUP BY location_id
            ) r ON r.location_id = l.location_id
            WHERE l.effective_as_of = r.last_set
        )

        , sightings_per_location_ AS (
            SELECT
                LOWER(location_id) AS location_id
                , SUM(IFF(blip_count > 0 AND portal_blip_count > 0, 1, 0)) AS n_portals_and_location
                , SUM(IFF(blip_count > 0 AND portal_blip_count = 0, 1, 0)) AS n_location_only
                , SUM(IFF(blip_count = 0 AND portal_blip_count > 0, 1, 0)) AS n_portal_only
            FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS
            WHERE
                datediff(minutes, start_time, end_time)/60 < 12 -- don't include very loooong sightings
                AND start_time >= dateadd(day, -30, CURRENT_DATE())
            GROUP BY location_id
        )

        , calibration_bundle_ AS (
            SELECT
                s.location_id
                , bp.name
                , COALESCE(c.rssi_cutoff_thresholded, 0) AS rssi_cutoff_thresholded

                -- troubleshooting raw values
                , COALESCE(c.rssi_cutoff, 0) AS rssi_cutoff
                , COALESCE(c.n_sightings, 0) AS n_sightings

                -- validity checks --> warnings & alerts
                , COALESCE(c.checks_n_sightings, true) AS checks_n_sightings
                , COALESCE(c.checks_high_suggested_threshold, false) AS checks_high_suggested_threshold
                , COALESCE(c.checks_low_suggested_threshold, false) AS checks_low_suggested_threshold
                , CASE
                    WHEN p.set_signal_strength IS NOT NULL AND c.rssi_cutoff_thresholded IS NOT NULL THEN abs(c.rssi_cutoff_thresholded - p.set_signal_strength) > 10
                    ELSE false
                  END AS checks_high_delta
                , s.n_portal_only
                , s.n_location_only
                , s.n_portals_and_location

            FROM sightings_per_location_ s
            LEFT JOIN rssi_cutoffs_ c ON c.location_id = s.location_id
            LEFT JOIN previous_config_ p ON p.location_id = s.location_id
            JOIN ZENPROD.CRM.PORTAL_BUSINESSPROFILE bp ON s.location_id = bp.business_id
        )

        SELECT *
        FROM calibration_bundle_
        WHERE (name NOT ILIKE '%qos%')
        ORDER BY location_id DESC
    ) AS thresholds ON (thresholds.location_id = calibration.LOCATION_ID)
    WHEN MATCHED THEN -- When above ON statement finds a match update the existing record
        UPDATE SET
            calibration.SIGNAL_STRENGTH = thresholds.rssi_cutoff_thresholded,
            calibration.UPDATED_AT = to_timestamp_ntz(CURRENT_TIMESTAMP()),
            calibration.PORTAL_ONLY_SIGHTINGS = thresholds.n_portal_only,
            calibration.LOCATION_ONLY_SIGHTINGS = thresholds.n_location_only,
            calibration.PORTAL_AND_LOCATION_SIGHTINGS = thresholds.n_portals_and_location,
            calibration.NEEDS_MANUAL_INTERVENTION = (
                -- no sightings with portal + location
                thresholds.n_portals_and_location = 0
                OR
                --  warnings & alerts
                thresholds.checks_low_suggested_threshold
                OR
                thresholds.checks_high_suggested_threshold
                OR
                -- |Δ| > 10dB
                thresholds.checks_high_delta
                -- less than 60 portal blips
                OR
                thresholds.checks_n_sightings
            ),
            calibration.MANUAL_INTERVENTION_REASON = (
            CASE
                WHEN thresholds.n_portals_and_location = 0 THEN 'no_sighting_portal_location'
                WHEN thresholds.checks_n_sightings THEN 'low_n_sightings'
                WHEN thresholds.checks_low_suggested_threshold THEN 'extreme_low'
                WHEN thresholds.checks_high_suggested_threshold THEN 'extreme_high'
                WHEN thresholds.checks_high_delta THEN 'high_delta'
                ELSE NULL
            END
            )

    WHEN NOT MATCHED THEN -- When above ON statement does not find a match insert as new record
        INSERT
        VALUES (
        thresholds.location_id,
        thresholds.rssi_cutoff_thresholded,
        (
            thresholds.n_portals_and_location = 0
            OR
            thresholds.checks_low_suggested_threshold
            OR
            thresholds.checks_high_suggested_threshold
            OR
            thresholds.checks_high_delta
            OR
            thresholds.checks_n_sightings
        ),
        CASE
            WHEN thresholds.n_portals_and_location = 0 THEN 'no_sighting_portal_location'
            WHEN thresholds.checks_n_sightings THEN 'low_n_sightings'
            WHEN thresholds.checks_low_suggested_threshold THEN 'extreme_low'
            WHEN thresholds.checks_high_suggested_threshold THEN 'extreme_high'
            WHEN thresholds.checks_high_delta THEN 'high_delta'
            ELSE NULL
        END,
        thresholds.n_portal_only,
        thresholds.n_location_only,
        thresholds.n_portals_and_location,
        to_timestamp_ntz(CURRENT_TIMESTAMP()),
        to_timestamp_ntz(CURRENT_TIMESTAMP())
);


ALTER TASK IF EXISTS  ZENPROD.PRESENCE.WIFI_CALCULATE_SIGNAL_STRENGTH_TASK RESUME;

