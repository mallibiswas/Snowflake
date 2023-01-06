-------------------------------------------------------
------------------ SUBSCRIPTIONADDONPRICE ------------------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTIONADDONPRICE;

copy into  &{dbname}._STAGING.SUBSCRIPTIONADDONPRICE
from
(
	select 
	$1::integer as subscriptionaddonprice_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as price_type,
	$5::number(18,2) as price,
	$6 as pricing_data,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscriptionaddonprice.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTIONADDONPRICE swap with &{dbname}.AMS.SUBSCRIPTIONADDONPRICE;
