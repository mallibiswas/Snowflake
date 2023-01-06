--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS as
		select
			$1 as id,
			$2 as zenreach_campaign_id,
			$3::text as platform_campaign_id,
			$4::text as location_id,
			$5::text as insight_type,
			$6::text as breakdown_type,
			$7::text as breakdown_value,
			$8::text as version,
			$9::bigint as revenue_generated_cents,
			$10::timestamp as created,
			$11::timestamp as updated,
			$12 as sync_event_id,
			$13::bigint as calculated_walkthroughs,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_campaigns_locations.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TASK resume;
