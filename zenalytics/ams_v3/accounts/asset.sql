use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Asset
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.asset as
select
$1 as asset_id,
$2 as account_id,
$3 as salesforce_asset_id,
$4 as subscription_id,
$5 as charge_id,
$6 as item_type,
$7::timestamp as created,
$8::timestamp as updated,
$9 as payment_info_id,
$10 as ads_io_id,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/asset.csv;

alter table &{stageschemaname}.asset swap with &{schemaname}.asset;
