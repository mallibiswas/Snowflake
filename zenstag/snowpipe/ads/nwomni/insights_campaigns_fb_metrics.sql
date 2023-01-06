--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_FB_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_FB_METRICS as
		select
			$1 as id,
			$2 as insights_campaigns_id,
			$3::bigint as impressions,
			$4::bigint as clicks,
			$5::bigint as engagement,
			$6::bigint as thru_plays,
			$7::bigint as video_plays,
			$8::bigint as link_clicks,
			$9::bigint as outbound_clicks,
			$10::real as click_through_rate,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_campaigns_fb_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_FB_METRICS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_FB_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_FB_METRICS_TASK resume;
