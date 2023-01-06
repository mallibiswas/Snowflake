--- Procedure to replace tables
create or replace procedure ZENPROD.ADS.MOCK_FB_AD_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENPROD.ADS.MOCK_FB_AD_ACCOUNTS as
		select
			$1::text as ad_account_id,
			current_timestamp() as asof_date
		FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/mock_fb_ad_accounts.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.MOCK_FB_AD_ACCOUNTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENPROD.ADS.MOCK_FB_AD_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.MOCK_FB_AD_ACCOUNTS_TASK resume;
