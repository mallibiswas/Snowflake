SELECT C.ACCOUNT_ID,
       substr(S.OPPORTUNITYID, 1, 15) AS SALESFORCE_OPPORTUNITY_ID,
       C.ID                           AS ADS_IO_ID,
       current_date                   AS ASOF_DATE
FROM {{ ref('stg_ams_account__campaign') }} c,
        {{ ref('stg_ams_account__asset') }} a,
        {{ ref('stg_ams_account__order_item') }} oi,
        {{ ref('stg_ams_account__orders' ) }} o,
        {{ ref('stg_ams_account__salesforce_quote') }} sq,
        {{ ref('stg_sfdc__quote') }} S
WHERE c.ID = a.CAMPAIGN_ID
  AND a.ID = oi.asset_id
  AND o.ID = oi.order_id
  AND o.SALESFORCE_QUOTE_UUID = sq.SALESFORCE_QUOTE_ID
  AND sq.salesforce_quote_id = S.id

