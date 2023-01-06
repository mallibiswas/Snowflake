
use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Unlimited Quota Accounts 
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.unlimited_quota_accounts 
as 
select 
$1 as unlimited_quota_account_id,
$2 as account_id,
$3::timestamp as created,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/unlimited_quota_accounts.csv;

alter table &{stageschemaname}.unlimited_quota_accounts swap with &{schemaname}.unlimited_quota_accounts;

