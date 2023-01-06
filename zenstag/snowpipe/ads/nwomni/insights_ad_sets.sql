--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.INSIGHTS_AD_SETS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.INSIGHTS_AD_SETS as
		select
			$1 as id,
			$2 as zenreach_campaign_id,
			$3::text as platform_campaign_id,
			$4::text as ad_set_id,
			$5::text as insight_type,
			$6::text as breakdown_type,
			$7::text as breakdown_value,
			$8::text as version,
			$9::bigint as spend_cents,
			$10::bigint as billable_cents,
			$11::bigint as revenue_generated_cents,
			$12::real as roas,
			$13::timestamp as created,
			$14::timestamp as updated,
			$15 as sync_event_id,
			$16::bigint as calculated_walkthroughs,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_ad_sets.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.INSIGHTS_AD_SETS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.INSIGHTS_AD_SETS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.INSIGHTS_AD_SETS_TASK resume;
