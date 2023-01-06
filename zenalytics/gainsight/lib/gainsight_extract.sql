-----------------------------------------------------------------------
--------------------- gainsight_acct_attributes -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

SET MAX_ASOF_DATE = (SELECT MAX(asof_date) FROM &{dbname}.&{schemaname}.&{tablename});

create or replace file format csv_unload_format
TYPE = csv 
FIELD_DELIMITER=','
ESCAPE_UNENCLOSED_FIELD='\\'  
ESCAPE = '\\'
RECORD_DELIMITER='\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
EMPTY_FIELD_AS_NULL=True
null_if=('') 
compression = none;

create or replace temporary stage my_unload_stage file_format = csv_unload_format;

copy into @my_unload_stage/unload/ 
from (
	select
        parent_id as Id,
        AMSAccountState as AMSAccountState,
        NVL(TRIM(StandardizedAccountName),'N/A') as StandardizedAccountName,
        NVL(ZenreachBusinessSegment,'N/A') as ZenreachBusinessSegment,
        NVL(RetentionAccountManager,'N/A') as RetentionAccountManager,
        NVL(CoreProductPackage,'N/A') as CoreProductPackage,
        TO_CHAR(NVL(LAST_ASOF_DATE,'1970-01-01'),'YYYY/MM/DD') as LastCustomAttrAsofDate,
        TO_CHAR(NVL($MAX_ASOF_DATE,'1970-01-01'),'YYYY/MM/DD') as AsofDate,
        NVL(SUM(TotalNumberOfLocations),0) as TotalNumberOfLocations,
        NVL(SUM(TotalEmailsCollectedAllTime),0) as TotalEmailsCollectedAllTime,
        NVL(SUM(EmailsCollectedLast31Days),0) as EmailsCollectedLast31Days,
        NVL(SUM(EmailsCollectedPrev31to62Days),0) as EmailsCollectedPrev31to62Days,
        ROUND(100*(NVL(SUM(EmailsCollectedLast31Days),0)-NVL(SUM(EmailsCollectedPrev31to62Days)+0.000001,0))/(NVL(SUM(EmailsCollectedPrev31to62Days),0)+0.000001),1) as PctChangeLastTwo31DayPeriods, 
        NVL(SUM(CurrentActiveEmailListSize),0) as CurrentActiveEmailListSize,
        NVL(SUM(CurrentAdssubscriptions),0) as CurrentAdssubscriptions,
        TO_CHAR(NVL(MAX(LastAdsSubsCancelledDate),'9999-12-31'),'YYYY/MM/DD') as LastAdsSubsCancelledDate,
        TO_CHAR(NVL(SUM(TotalEmailsCollectedAllTime),0)) as TotalEmailsCollectedAllTimeText,
        TO_CHAR(NVL(SUM(EmailsCollectedLast31Days),0)) as EmailsCollectedLast31DaysText,
        TO_CHAR(NVL(SUM(EmailsCollectedPrev31to62Days),0)) as EmailsCollectedPrev31to62DaysText,
        TO_CHAR(NVL(SUM(CurrentActiveEmailListSize),0)) as CurrentActiveEmailListSizeText,
        MAX(NVL(NVISITORS,0)) as NVISITORS,
        MAX(NVL(NINBUSINESSNETWORK,0)) as NINBUSINESSNETWORK,
        MAX(NVL(PERCINVISIBLE,0)) as PERCINVISIBLE,
        MAX(NVL(MEDIANVISITCOUNT,0)) as MEDIANVISITCOUNT,
        MAX(NVL(POTENTIALVALUE,0)) as POTENTIALVALUE,
        MAX(NVL(WALKBYMERCHANTYTD,0)) as WALKBYMERCHANTYTD,
        MAX(NVL(WALKBYNETWORKYTD,0)) as WALKBYNETWORKYTD,
        MAX(NVL(WALKBYMERCHANT30D,0)) as WALKBYMERCHANT30D,
        MAX(NVL(WALKBYNETWORK30D,0)) as WALKBYNETWORK30D         
        from &{dbname}.&{schemaname}.&{tablename} 
	where asof_date = to_date($MAX_ASOF_DATE)
        group by 1,2,3,4,5,6,7
)


file_format = (format_name = 'csv_unload_format') header=True overwrite=True; 

list @my_unload_stage;

get @my_unload_stage/unload/ file://&{datadir};
