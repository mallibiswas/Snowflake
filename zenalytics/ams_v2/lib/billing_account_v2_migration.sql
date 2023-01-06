-----------------------------------------------------------------------
------------------ BILLING_ACCOUNT_V2_MIGRATION -----------------------
-----------------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.BILLING_ACCOUNT_V2_MIGRATION;

copy into  &{dbname}._STAGING.BILLING_ACCOUNT_V2_MIGRATION
from
(
select 
	$1::integer as billing_account_migration_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as billing_account_v1_id,
	$5::integer as billing_account_v2_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/billing_account_v2_migration.csv
);

alter table &{dbname}._STAGING.BILLING_ACCOUNT_V2_MIGRATION swap with &{dbname}.AMS.BILLING_ACCOUNT_V2_MIGRATION;
