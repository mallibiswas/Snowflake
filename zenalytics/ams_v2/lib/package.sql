---------------------------------------------------------------
------------------ PACKAGE ----------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.PACKAGE;

copy into  &{dbname}._STAGING.PACKAGE
from
(
	select 
	$1::integer as package_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as product_id,
	$5 as name,
	$6::boolean as active,
	$7::number(18,2) as monthly_list_price,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/package.csv
);

alter table &{dbname}._STAGING.PACKAGE swap with &{dbname}.AMS.PACKAGE;
