-------------------------------------------------------------------
---------- All invoices and adjustments from Recurly 
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};


create or replace table &{stageschemaname}.d_recurly_invoices  
as  
select ra.account_code, 
        rs.recurly_subscription_id, 
        ri.collection_method as payment_method,
        ri.state as invoice_state,
        ra.invoice_number, 
        ra.origin as recurly_adjustment_origin,
        ra.subtotal as recurly_subtotal,
        ra.product_code as recurly_product_code,
        ri.paid as invoice_paid,
        ri.origin as invoice_origin,
        ri.type as invoice_type,
        ri.created_at as invoice_created_at, 
        ri.closed_at as invoice_closed_at,  
       CASE
          WHEN lower(ra.description)  LIKE '%hardware%'     THEN 'Hardware'
          WHEN lower(description)  LIKE '%installation%' THEN 'Installation'
          WHEN lower(product_code) LIKE '%ads%'          THEN 'Ads'
        ELSE 'Core'
       END Product_Category,
	current_date as asof_date
from 	recurly.recurly_subscriptions rs, 
	recurly.recurly_adjustments ra, 
	recurly.recurly_invoices ri 
where 	ra.invoice_id = ri.invoice_id
and 	ra.subscription_id = rs.subscription_id
;

alter table &{stageschemaname}.d_recurly_invoices swap with &{schemaname}.d_recurly_invoices;

