--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.MOCK_FB_AD_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.MOCK_FB_AD_ACCOUNTS as
		select
			$1::text as ad_account_id,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/mock_fb_ad_accounts.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.MOCK_FB_AD_ACCOUNTS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.MOCK_FB_AD_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.MOCK_FB_AD_ACCOUNTS_TASK resume;
