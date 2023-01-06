SELECT  $1                  AS id
       ,$2                  AS salesforce_quote_uuid
       ,$3                  AS salesforce_quote_line_item_id
       ,$4                  AS asset_id
       ,$5                  AS replacement_quote_line_id
       ,$6                  AS product2_id
       ,$7                  AS product2_name
       ,$8                  AS product2_code
       ,$9                  AS product_family
       ,$10                 AS product_sku
       ,$11                 AS product_category
       ,$12::boolean        AS invoice_now
       ,$13::number         AS billing_frequency_months
       ,$14                 AS pricebook_entry_id
       ,$15::number         AS quantity
       ,$16::number         AS unit_price_cents
       ,$17::number         AS discount_percent
       ,$18::number         AS total_price_cents
       ,$19::timestamp      AS start_date
       ,$20::timestamp      AS end_date
       ,$21::timestamp      AS created
       ,$22::timestamp      AS updated
       ,$23::number         AS margin_percent
       ,$24                 AS description
       ,$25                 AS internal_description
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/salesforce_quote_line_item.csv') }}
