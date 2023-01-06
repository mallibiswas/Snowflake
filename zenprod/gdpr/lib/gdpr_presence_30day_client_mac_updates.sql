use warehouse &{whname};
use database &{dbname};
use schema &{schemaname};
use role &{rolename};

--
-- Set lookback window for incremental scans
--

ALTER SESSION SET TIMEZONE = 'UTC';

SET MIN_UNIX_TS = (select DATE_PART(&{epoch}, to_timestamp_ntz(dateadd(day, -35, current_date()))));
SET MAX_UNIX_TS = (select DATE_PART(&{epoch}, to_timestamp_ntz(dateadd(day, -30, current_date()))));

-- null out client macs for unconsented records older than 30 days
update &{dbname}.&{schemaname}.&{tablename} es
set    client_mac = Null
where  contact_id is null -- unconsented
and    &{datefield} >= $MIN_UNIX_TS -- younger than 35 days
and    &{datefield} < $MAX_UNIX_TS; -- older than 30 days
;

~
~

