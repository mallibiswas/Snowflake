WITH INVOICES AS (
    SELECT *
    FROM {{ ref('stg_recurly__invoices') }}
    WHERE STATE != 'voided'
),

     PARSED_ROWS AS (
         SELECT INVOICES.*
              , FLATTENED_ADJUSTMENTS.VALUE PARSED_ADJUSTMENT
         FROM INVOICES
            , TABLE (flatten(GET_PATH(parse_json(LINE_ITEMS),'data'))) FLATTENED_ADJUSTMENTS
     )
SELECT PARSED_ROWS.ACCOUNT_CODE                                                ACCOUNT__CODE
     , PARSED_ROWS.COMPANY_NAME                                                ACCOUNT__COMPANY
     , PARSED_ADJUSTMENT:subscription_id::TEXT                                 ADJUSTMENT__SUBSCRIPTION_ID
     , PARSED_ROWS.INVOICE_NUMBER                                              INVOICE__NUMBER
     , PARSED_ROWS.COLLECTION_METHOD                                           INVOICE__COLLECTION_METHOD
     , PARSED_ROWS.ORIGIN                                                      INVOICE__ORIGIN
     , PARSED_ROWS.DISCOUNT                                                    INVOICE__DISCOUNT
     , PARSED_ROWS.SUBTOTAL                                                    INVOICE__SUBTOTAL
     , PARSED_ROWS.TOTAL                                                       INVOICE__TOTAL
     , PARSED_ROWS.PAID                                                        INVOICE__PAID
     , PARSED_ROWS.BALANCE                                                     INVOICE__BALANCE
     , PARSED_ROWS.CREATED_AT                                                  INVOICE__DATE_CREATED
     , PARSED_ROWS.CREATED_AT::TIMESTAMP                                       INVOICE__DATE_CREATED_TIMESTAMP
     , PARSED_ROWS.CLOSED_AT                                                   INVOICE__DATE_CLOSED
     , PARSED_ADJUSTMENT:invoice_href::TEXT                                    ADJUSTMENT__INVOICE_ID
     , PARSED_ADJUSTMENT:state::TEXT                                           ADJUSTMENT__STATE
     , PARSED_ADJUSTMENT:accounting_code::TEXT                                 ADJUSTMENT__ACCOUNTING_CODE
     , PARSED_ADJUSTMENT:revenue_schedule_type::TEXT                           ADJUSTMENT__REVENUE_SCHEDULE_TYPE
     , PARSED_ADJUSTMENT:currency::TEXT                                        ADJUSTMENT__CURRENCY
     , PARSED_ADJUSTMENT:description::TEXT                                     ADJUSTMENT__DESCRIPTION
     , PARSED_ADJUSTMENT:created_at::TIMESTAMP                                 ADJUSTMENT__ACCOUNT_CREATED_AT
     , PARSED_ADJUSTMENT:updated_at::TIMESTAMP                                 ADJUSTMENT__UPDATED_AT
     , PARSED_ADJUSTMENT:uuid::TEXT                                            ADJUSTMENT__UUID
     , PARSED_ADJUSTMENT:origin::TEXT                                          ADJUSTMENT__ORIGIN
     , PARSED_ADJUSTMENT:refund::TEXT                                          ADJUSTMENT__REFUND
     , PARSED_ADJUSTMENT:product_code::TEXT                                    ADJUSTMENT__PRODUCT_CODE
     , PARSED_ADJUSTMENT:start_date::TIMESTAMP                                 ADJUSTMENT__START_TIMESTAMP
     , to_date(PARSED_ADJUSTMENT:start_date::TIMESTAMP)                        ADJUSTMENT__START_DATE
     , PARSED_ADJUSTMENT:end_date::TIMESTAMP                                   ADJUSTMENT__END_TIMESTAMP
     , to_date(PARSED_ADJUSTMENT:end_date::TIMESTAMP)                          ADJUSTMENT__END_DATE
     , PARSED_ADJUSTMENT:taxable::TEXT                                         ADJUSTMENT__TAXABLE
     , PARSED_ADJUSTMENT:tax_in_cents::DECIMAL                                 ADJUSTMENT__TAX_IN_CENTS
     , PARSED_ADJUSTMENT:unit_amount::DECIMAL                                  ADJUSTMENT__UNIT_AMOUNT
     , PARSED_ADJUSTMENT:quantity::INTEGER                                     ADJUSTMENT__QUANTITY
     , PARSED_ADJUSTMENT:subtotal::DECIMAL                                     ADJUSTMENT__SUBTOTAL
     , PARSED_ADJUSTMENT:discount::DECIMAL                                     ADJUSTMENT__DISCOUNT
     , PARSED_ADJUSTMENT:amount::DECIMAL                                       ADJUSTMENT__AMOUNT

     , CASE
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%hardware%' THEN 'Hardware'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%installation fee%' THEN 'Installation'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%intallation fee%' THEN 'Installation'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%setup fee%' THEN 'Installation'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%ads%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__PRODUCT_CODE) LIKE '%ads%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%attract%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__PRODUCT_CODE) LIKE '%attract%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%variable ad spend%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%promotion%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%advertising by zenreach%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%facebook%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%programmatic%' THEN 'Attract'
           WHEN lower(ADJUSTMENT__DESCRIPTION) LIKE '%mobile ctv%' THEN 'Attract'
           ELSE 'Engage'
    END                                                                        ADJUSTMENT__PRODUCT_CATEGORY

     , CASE
           WHEN ADJUSTMENT__SUBSCRIPTION_ID IS NOT NULL THEN 'Subscription'
           ELSE 'One-time'
    END AS                                                                     ADJUSTMENT__INVOICE_TYPE

     , CASE
           WHEN ACCOUNT__CODE = '8a2f606a-ed45-47cb-bd9e-13458296ef11' AND
                ADJUSTMENT__INVOICE_TYPE = 'One-time' AND ADJUSTMENT__PRODUCT_CATEGORY = 'Attract'
               THEN 'Arrears' -- Postmates
           WHEN ads_billing_log.CHARGE_ID IS NULL THEN 'Advance'
           ELSE 'Arrears'
    END AS                                                                     BILLING_TIMING

     , CASE
           WHEN BILLING_TIMING = 'Advance' THEN ADJUSTMENT__PRODUCT_CATEGORY
           WHEN BILLING_TIMING = 'Arrears' THEN 'Attract IO'
           ELSE 'Unknown'
    END AS                                                                     ADJUSTMENT__PRODUCT_FAMILY

     , CASE
           WHEN to_timestamp(ADJUSTMENT__START_DATE) < ADJUSTMENT__START_TIMESTAMP
               THEN ADJUSTMENT__START_DATE + 1
           ELSE ADJUSTMENT__START_DATE
    END AS                                                                     ADJUSTMENT__START_DATE_ADJUSTED
     , CASE
           WHEN (BILLING_TIMING = 'Arrears') THEN ADJUSTMENT__END_DATE + 1
           WHEN ADJUSTMENT__END_DATE IS NULL THEN ADJUSTMENT__START_DATE_ADJUSTED
           WHEN to_timestamp(ADJUSTMENT__START_DATE) = ADJUSTMENT__START_TIMESTAMP
               THEN greatest(ADJUSTMENT__END_DATE - 1, ADJUSTMENT__START_DATE_ADJUSTED) -- GREATEST on adjustment__start_date_adjusted controls for 1 or 2 day invoices. This prevents an invoice from having a negative length based off of adjusted start date.
           ELSE greatest(ADJUSTMENT__END_DATE, ADJUSTMENT__START_DATE_ADJUSTED)
    END AS                                                                     ADJUSTMENT__END_DATE_ADJUSTED

     , plans.INTERVAL_LENGTH__IT

     , datediff(DAY, ADJUSTMENT__START_DATE_ADJUSTED, ADJUSTMENT__END_DATE_ADJUSTED) +
       1                                                                       ADJUSTMENT__LENGTH_DAYS_ADJUSTED_INCLUSIVE
     , datediff(MONTH, ADJUSTMENT__START_DATE_ADJUSTED,
                ADJUSTMENT__END_DATE_ADJUSTED)                                 ADJUSTMENT__LENGTH_MONTHS

     , CASE
           WHEN ADJUSTMENT__LENGTH_DAYS_ADJUSTED_INCLUSIVE <= 31 THEN 1
           WHEN ADJUSTMENT__LENGTH_DAYS_ADJUSTED_INCLUSIVE > 31 AND
                ADJUSTMENT__LENGTH_DAYS_ADJUSTED_INCLUSIVE <= 62 THEN 2
           WHEN ADJUSTMENT__LENGTH_DAYS_ADJUSTED_INCLUSIVE > 62 AND
                ADJUSTMENT__LENGTH_DAYS_ADJUSTED_INCLUSIVE <= 92 THEN 3
           ELSE ADJUSTMENT__LENGTH_MONTHS
    END AS                                                                     ADJUSTMENT_LENGTH_DAYS_BACKFILL

     , nvl(plans.INTERVAL_LENGTH__IT, ADJUSTMENT_LENGTH_DAYS_BACKFILL) ADJUSTMENT_MONTHS

     , ADJUSTMENT__AMOUNT / nvl(ADJUSTMENT_MONTHS, 1)                          ADJUSTMENT_MRR

FROM PARSED_ROWS
             LEFT JOIN {{ ref('stg_recurly__plans') }} plans
                       ON plans.CODE = PARSED_ADJUSTMENT:product_code::TEXT
             LEFT JOIN {{ ref('stg_ams_account__ads_billing_log') }} ads_billing_log
                       ON ads_billing_log.CHARGE_ID = PARSED_ADJUSTMENT:uuid::TEXT
WHERE
    -- Exclude Credits that result in write-offs. Be careful that this does not also exclude IO Billings.
    NOT (
                date_trunc('month', INVOICE__DATE_CREATED) >
                nvl(date_trunc('month', ADJUSTMENT__END_DATE), date_trunc('month', ADJUSTMENT__START_DATE))
            AND ads_billing_log.CHARGE_ID IS NULL
            AND ACCOUNT__CODE != '8a2f606a-ed45-47cb-bd9e-13458296ef11' -- PM which didn't have a billing log
        )
