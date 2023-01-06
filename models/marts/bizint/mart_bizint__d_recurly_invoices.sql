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
from {{ ref('stg_recurly__subscriptions') }} rs,
	{{ ref('stg_recurly__adjustments') }} ra,
	{{ ref('stg_recurly__invoices') }} ri
where 	ra.invoice_id = ri.invoice_id
and 	ra.subscription_id = rs.subscription_id
