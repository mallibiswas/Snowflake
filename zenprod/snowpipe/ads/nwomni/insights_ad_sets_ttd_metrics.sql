--- Procedure to replace tables
create or replace procedure ZENPROD.ADS.INSIGHTS_AD_SETS_TTD_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENPROD.ADS.INSIGHTS_AD_SETS_TTD_METRICS as
		select
			$1 as id,
			$2 as insights_ad_sets_id,
			$3::bigint as impressions,
			$4::bigint as impression_uniques,
			$5::bigint as clicks,
			$6::bigint as cost_cents,
			$7::bigint as player_starts,
			$8::bigint as player25perc_complete,
			$9::bigint as player50perc_complete,
			$10::bigint as player75perc_complete,
			$11::bigint as player_completed_views,
			$12::bigint as sampled_tracked_impressions,
			$13::bigint as sampled_viewed_impressions,
			$14::real as click_through_rate,
			current_timestamp() as asof_date
		FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_ad_sets_ttd_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.INSIGHTS_AD_SETS_TTD_METRICS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENPROD.ADS.INSIGHTS_AD_SETS_TTD_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.INSIGHTS_AD_SETS_TTD_METRICS_TASK resume;
