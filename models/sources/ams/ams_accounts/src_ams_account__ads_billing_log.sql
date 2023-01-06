SELECT  $1                  AS id
       ,$2                  AS campaign_id
       ,$3::date            AS start_date
       ,$4::date            AS end_date
       ,$5::number          AS total_ads_spend
       ,$6::number          AS total_billed_cent
       ,$7                  AS charge_id
       ,$8                  AS error
       ,$9::timestamp       AS created
       ,$10::timestamp      AS updated
       ,$11::number         AS total_spend_with_margin_cents
       ,$12::number         AS total_spend_before_cap_cents
       ,$13::number         AS previous_billed_cents
       ,$14::number         AS io_budget_cents
       ,$15:: number        AS billing_month
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_billing_log.csv') }}
