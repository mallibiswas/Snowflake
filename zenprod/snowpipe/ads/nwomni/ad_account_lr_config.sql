--- Procedure to replace tables
create or replace procedure ZENPROD.ADS.AD_ACCOUNT_LR_CONFIG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENPROD.ADS.AD_ACCOUNT_LR_CONFIG as
		select
			$1 as ad_account_id,
			$2::text as default_attribution_window,
			current_timestamp() as asof_date
		FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/ad_account_lr_config.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.AD_ACCOUNT_LR_CONFIG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENPROD.ADS.AD_ACCOUNT_LR_CONFIG_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.AD_ACCOUNT_LR_CONFIG_TASK resume;
