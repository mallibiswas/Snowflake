--- Procedure to replace tables
create or replace procedure ZENSAND.ADS.INSIGHTS_CAMPAIGNS_TTD_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSAND.ADS.INSIGHTS_CAMPAIGNS_TTD_METRICS as
		select
			$1 as id,
			$2 as insights_campaigns_id,
			$3::bigint as impressions,
			$4::bigint as impression_uniques,
			$5::bigint as clicks,
			$6::bigint as cost_cents,
			$7::real as click_through_rate,
			current_timestamp() as asof_date
		FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_campaigns_ttd_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.INSIGHTS_CAMPAIGNS_TTD_METRICS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSAND.ADS.INSIGHTS_CAMPAIGNS_TTD_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.INSIGHTS_CAMPAIGNS_TTD_METRICS_TASK resume;
