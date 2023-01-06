use warehouse &{whname};
use database &{dbname};
use role &{rolename};

--------------------------------------------------------------------
-------------- Salesforce Account
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.salesforce_account as 
select
$1 as salesforce_account_id,
$2 as name,
$12::timestamp as created,
$13::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/salesforce_account.csv; 

alter table &{stageschemaname}.salesforce_account swap with &{schemaname}.salesforce_account;

--------------------------------------------------------------------
-------------- Salesforce Asset
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.salesforce_asset as 
select
$1 as salesforce_asset_id,
$2 as parent_id,
$3::integer as quantity,
$4::integer as unit_price_cents,
$5::timestamp as installed_date,
$6::timestamp as purchase_date,
$7::timestamp as termination_date,
$8::timestamp as created,
$9::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/salesforce_asset.csv; 

alter table &{stageschemaname}.salesforce_asset swap with &{schemaname}.salesforce_asset;

--------------------------------------------------------------------
-------------- Salesforce Order
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.salesforce_order as 
select
$1 as salesforce_order_id,
$2 as status,
$3 as type,
$4::timestamp as effective_date,
$5::timestamp as created,
$6::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/salesforce_order.csv; 

alter table &{stageschemaname}.salesforce_order swap with &{schemaname}.salesforce_order;

--------------------------------------------------------------------
-------------- Salesforce Order Item
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.salesforce_order_item as 
select
$1 as salesforce_order_item_id,
$2 as replacement_order_item_id,
$3::integer as quantity,
$4::timestamp as created,
$5::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/salesforce_order_item.csv; 

alter table &{stageschemaname}.salesforce_order_item swap with &{schemaname}.salesforce_order_item ;

--------------------------------------------------------------------
-------------- Salesforce Quote
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.salesforce_quote as 
select
$1 as salesforce_quote_key,
$2 as salesforce_quote_id,
$3 as billing_city,
$4 as billing_country,
$5 as billing_state,
$6 as billing_street,
$7 as billing_postal_code,
$8 as billing_method,
$9 as contact_id,
$10 as contact_email,
$11 as contact_name,
$12 as contract_id,
$13 as pricebook2_id,
$14 as auto_sign,
$15 as status,
$16 as social_media_accounts,
$17::timestamp as created,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/salesforce_quote.csv; 

alter table &{stageschemaname}.salesforce_quote swap with &{schemaname}.salesforce_quote;

--------------------------------------------------------------------
-------------- Salesforce Quote Line Item
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.salesforce_quote_line_item as 
select
$1 as salesforce_quote_line_item_key,
$2 as salesforce_quote_key,
$3 as salesforce_quote_line_item_id,
$4 as salesforce_asset_id,
$5 as replacement_quote_line_item_id,
$6 as product2_id,
$7 as product2_name,
$8 as product2_code,
$9 as product_family,
$10 as product_sku,
$11 as product_category,
$12::boolean as invoice_now,
$13::integer as billing_frequency_months,
$14 as pricebook_entry_id,
$15::integer as quantity,
$16::integer as unit_price_cents,
$17::float as discount_percent,
$18::integer as total_price_cents,
$19::timestamp as start_date,
$20::timestamp as end_date,
$21::timestamp as created,
$22::timestamp as updated,
to_date('&{asof_date}') as asof_date 
FROM @&{stagename}/&{stagepath}/salesforce_quote_line_item.csv; 

alter table &{stageschemaname}.salesforce_quote_line_item swap with &{schemaname}.salesforce_quote_line_item;

--------------------------------------------------------------------
-------------- Quote Line Item Info
--------------------------------------------------------------------

create or replace transient table &{stageschemaname}.quote_line_item_info as 
select
$1 as quote_line_item_info_id,
$2 as payment_info_id,
$3 as salesforce_quote_line_item_id,
$4::timestamp as created,
to_date('&{asof_date}') as asof_date
FROM @&{stagename}/&{stagepath}/quote_line_item_info.csv;

alter table &{stageschemaname}.quote_line_item_info swap with &{schemaname}.quote_line_item_info;
