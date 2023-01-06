SELECT D.ADJUSTMENT__PRODUCT_CATEGORY
     , D.ADJUSTMENT__PRODUCT_FAMILY
     , D.AMS_ACCOUNT_ID
     , D.ACCOUNT_NAME
     , D.EARLIEST_START
     , D.FINAL
     , D.MOVEMENT_DATE
     , D.BILLING_TIMING                                                 -- Temporary
     , ARRAY_SUBSET.EXPIRES_AT
     , ARRAY_SUBSET.ADJUSTMENT__START_DATE
     , ARRAY_SUBSET.ADJUSTMENT__END_DATE
     , DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1)) MD_TEMPORARY --Temporary
     , CASE
    -- Subscriptions billed In Advance will be considered Churn once last ED is expired
           WHEN (D.BILLING_TIMING = 'Advance') AND (ARRAY_SUBSET.ADJUSTMENT__END_DATE IS NULL) AND
                (D.MOVEMENT_DATE < CURRENT_DATE()) THEN 0
           WHEN (D.BILLING_TIMING = 'Arrears') AND (ARRAY_SUBSET.ADJUSTMENT__END_DATE IS NULL) AND
                (D.MOVEMENT_DATE < DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))) THEN 0
           ELSE ARRAY_SUBSET.ADJUSTMENT__AMOUNT
    END             AS                                     SUBTOTAL_AMOUNT
     , CASE
           WHEN (ARRAY_SUBSET.ADJUSTMENT__END_DATE = MOVEMENT_DATE) AND
                (ARRAY_SUBSET.ADJUSTMENT__END_DATE >= ARRAY_SUBSET.EXPIRES_AT) THEN 0
           ELSE ARRAY_SUBSET.ADJUSTMENT_MRR
    END             AS                                     MRR
     , current_date AS                                     ASOF_DATE
FROM {{ ref('mart_revrec__d_account_service_dates') }} D
         LEFT JOIN {{ ref('mart_revrec__recurly_invoices_to_subscriptions') }} ARRAY_SUBSET
                   ON ARRAY_SUBSET.AMS_ACCOUNT_ID = D.AMS_ACCOUNT_ID
                       AND ARRAY_SUBSET.ADJUSTMENT__PRODUCT_CATEGORY = D.ADJUSTMENT__PRODUCT_CATEGORY
                       AND ARRAY_SUBSET.ADJUSTMENT__PRODUCT_FAMILY = D.ADJUSTMENT__PRODUCT_FAMILY
                       AND ARRAY_SUBSET.ADJUSTMENT__START_DATE <= MOVEMENT_DATE
                       AND ARRAY_SUBSET.ADJUSTMENT__END_DATE > MOVEMENT_DATE
     -- one exception would be if it has a start and end date of the same day. In that scenario, ED would not be > MD -- Maybe: OR( SD <= MD and ED = SD)
WHERE (
              SUBTOTAL_AMOUNT IS NOT NULL
              OR MRR IS NOT NULL
          )