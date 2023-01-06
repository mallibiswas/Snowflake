SELECT CREDIT_PAYMENTS[0]                                     AS CREDIT_PAYMENTS,
       LINE_ITEMS: DATA [0]                                   AS LINE_ITEMS,
       SUBSCRIPTION_IDS[0]::STRING                            AS SUBSCRIPTION_ID,
       NUMBER                                                 AS INVOICE_NUMBER,
       STATE,
       NVL(TOTAL, TOTAL__DE)::NUMBER(38, 2)                   AS TOTAL,
       CREDIT_PAYMENTS[0]:original_invoice: NUMBER::INTEGER   AS ORIGINAL_INVOICE_NUMBER,
       CREDIT_PAYMENTS[0]:applied_to_invoice: NUMBER::INTEGER AS APPLIED_TO_INVOICE_NUMBER,
       CREDIT_PAYMENTS[0]:amount::NUMBER(38, 2)               AS ADJUSTMENT_AMOUNT,
       CREDIT_PAYMENTS[0]: OBJECT::STRING                     AS ADJUSTMENT_TYPE,
       LINE_ITEMS: DATA [0]:origin::STRING                    AS ORIGIN,
       LINE_ITEMS: DATA [0]:description::STRING               AS DECRIPTION,
       LINE_ITEMS: DATA [0]:start_date::DATE                  AS START_DATE,
       LINE_ITEMS: DATA [0]:end_date::DATE                    AS END_DATE,
       LINE_ITEMS: DATA [0]:unit_amount::NUMBER(38, 2)        AS UNIT_AMOUNT,
       TRANSACTIONS[0]:uuid::STRING                           AS TRANSACTION_UUID
FROM {{ source('RECURLY', 'INVOICES') }} I
WHERE CREDIT_PAYMENTS[0] IS NOT NULL
  AND STATE = 'paid'