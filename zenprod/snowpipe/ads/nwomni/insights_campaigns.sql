--- Procedure to replace tables
create or replace procedure ZENPROD.ADS.INSIGHTS_CAMPAIGNS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENPROD.ADS.INSIGHTS_CAMPAIGNS as
		select
			$1 as id,
			$2 as zenreach_campaign_id,
			$3::text as platform_campaign_id,
			$4::text as insight_type,
			$5::text as breakdown_type,
			$6::text as breakdown_value,
			$7::text as version,
			$8::bigint as spend_cents,
			$9::bigint as billable_cents,
			$10::bigint as revenue_generated_cents,
			$11::real as roas,
			$12::timestamp as created,
			$13::timestamp as updated,
			$14 as sync_event_id,
			$15::bigint as calculated_walkthroughs,
			current_timestamp() as asof_date
		FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_campaigns.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.INSIGHTS_CAMPAIGNS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENPROD.ADS.INSIGHTS_CAMPAIGNS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.INSIGHTS_CAMPAIGNS_TASK resume;
