--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.INSIGHTS_AD_SETS_FB_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.INSIGHTS_AD_SETS_FB_METRICS as
		select
			$1 as id,
			$2 as insights_ad_sets_id,
			$3::bigint as impressions,
			$4::bigint as clicks,
			$5::bigint as engagement,
			$6::bigint as thru_plays,
			$7::bigint as video_plays_at25_perc,
			$8::bigint as video_plays_at50_perc,
			$9::bigint as video_plays_at75_perc,
			$10::bigint as video_plays_at95_perc,
			$11::bigint as video_plays_at100_perc,
			$12::bigint as video_plays,
			$13::bigint as video_average_play_time,
			$14::bigint as link_clicks,
			$15::bigint as outbound_clicks,
			$16::bigint as e_cpm_cents,
			$17::real as click_through_rate,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_ad_sets_fb_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.INSIGHTS_AD_SETS_FB_METRICS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.INSIGHTS_AD_SETS_FB_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.INSIGHTS_AD_SETS_FB_METRICS_TASK resume;
