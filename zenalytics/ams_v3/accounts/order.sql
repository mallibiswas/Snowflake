use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Order
--------------------------------------------------------------------

-- need to quote order since it is a keyword
create or replace transient table &{stageschemaname}."order" as 
select
$1 as order_id,
$2 as account_id,
$3 as salesforce_quote_key,
$4 as salesforce_order_id,
$5 as signer_name,
$6::timestamp as signed_date,
$7 as hardcopy_url,
$8::timestamp as created,
$9::timestamp as updated,
$10::timestamp as cancelled,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/order.csv; 

alter table  &{stageschemaname}."order" swap with  &{schemaname}."order";


--------------------------------------------------------------------
-------------- Order Item
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.order_item as 
select
$1 as order_item_id,
$2 as order_id,
$3 as asset_id,
$4 as salesforce_order_item_id,
$5 as salesforce_quote_line_item_key,
$6::boolean as salesforce_asset_synced,
$7::boolean as dirty,
$8::timestamp as created,
$9::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/order_item.csv; 

alter table &{stageschemaname}.order_item swap with &{schemaname}.order_item;
