--- Procedure to replace tables
create or replace procedure ZENSAND.ADS.INSIGHTS_CAMPAIGNS_WEEKLY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSAND.ADS.INSIGHTS_CAMPAIGNS_WEEKLY as
		select
			$1 as id,
			$2 as zenreach_campaign_id,
			$3::text as platform_campaign_id,
			$4::date as week_start_date,
			$5::date as week_end_date,
			$6::integer as days_live,
			$7::bigint as spend_cents,
			$8::bigint as billable_cents,
			$9::bigint as calculated_walkthroughs,
			$10::bigint as cost_per_walkthrough_cents,
			$11::real as roas,
			$12::bigint as revenue_generated_cents,
			$13::bigint as impressions,
			$14::bigint as clicks,
			$15::timestamp as created,
			$16::timestamp as updated,
			$17 as sync_event_id,
			$18::real as click_through_rate,
			current_timestamp() as asof_date
		FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_campaigns_weekly.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.INSIGHTS_CAMPAIGNS_WEEKLY_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSAND.ADS.INSIGHTS_CAMPAIGNS_WEEKLY_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.INSIGHTS_CAMPAIGNS_WEEKLY_TASK resume;
