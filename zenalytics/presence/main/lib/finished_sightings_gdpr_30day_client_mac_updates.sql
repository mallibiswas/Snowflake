use warehouse &{whname};
use database &{tgtdbname};
use schema &{tgtschemaname};
use role &{rolename};

--
-- Set lookback window for incremental scans
--

ALTER SESSION SET TIMEZONE = 'UTC';

SET MIN_UNIX_TS = (select to_timestamp_ntz(dateadd(day, -35, current_date())));
SET MAX_UNIX_TS = (select to_timestamp_ntz(dateadd(day, -30, current_date())));

-- null out client macs for unconsented records older than 30 days
update &{tgtdbname}.&{tgtschemaname}.&{tgttablename} es
set    client_mac_info = Null
where  contact_id is null -- unconsented
and    &{datefield} >= $MIN_UNIX_TS -- younger than 35 days
and    &{datefield} < $MAX_UNIX_TS; -- older than 30 days
;

~
~

