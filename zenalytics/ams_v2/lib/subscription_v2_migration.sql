---------------------------------------------------------------
------------------ SUBSCRIPTION_V2_MIGRATION ------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTION_V2_MIGRATION;

copy into  &{dbname}._STAGING.SUBSCRIPTION_V2_MIGRATION
from
(
	select 
	$1::integer as subscription_migration_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as contract_v1_id,
	$5::integer as subscription_v2_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscription_v2_migration.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTION_V2_MIGRATION swap with &{dbname}.AMS.SUBSCRIPTION_V2_MIGRATION;
