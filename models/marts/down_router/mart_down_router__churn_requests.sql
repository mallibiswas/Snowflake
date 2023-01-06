SELECT REQ.ID            AS SFDC_CHURN_REQ_ID,
       REQ.CREATEDDATE   AS CHURN_REQ_DATE,
       USERS.NAME        AS CREATED_BY,
       REQ.CHURN_STATUS__C,
       REQ.ACCOUNT_MRR__C,
       REQ.UNCOVERED_BY_PROGRAMMATIC_OUTREACH__C,
       REQ.UNCOVERED_BY_LOCAL_ADS__C,
       REQ.UNABLE_TO_REACH__C,
       REQ.NO_REASON_GIVEN__C,
       REQ.EARLY_TERMINATION__C,
       REQ.REASON_FOR_EARLY_TERMINATION__C,
       REQ.CHURN_REASON__C,
       REQ.CHURN_REASON_BUSINESS_SITUATION__C,
       REQ.CHURN_REASON_TECHNICAL__C,
       REQ.CHURN_REASON_SERVICE__C,
       REQ.CHURN_REASON_CONTRACT_BILLING__C,
       REQ.CHURN_REASON_PRODUCT__C,
       REQ.CHURN_REASON_DETAILS__C,
       REQ.MARKETING_PLATFORMS__C,
       REQ.OTHER_MARKETING_PLATFORMS__C,
       REQ.CONTACT_EMAIL_ADDRESS__C,
       REQ.NUMBER_OF_LOCATIONS__C,
       REQ.OPEN_TO_REACTIVATION__C,
       REQ.ESCALATED__C,
       COALESCE(AMS_LOC_1.LOCATION_ID, AMS_LOC_2.LOCATION_ID, AMS_LOC_3.LOCATION_ID, AMS_LOC_4.LOCATION_ID,
                AMS_LOC_5.LOCATION_ID, NULL) AS LOCATION_ID,
       current_date                          AS ASOF_DATE
FROM {{ ref('stg_sfdc__churn_request__c') }} REQ
         LEFT JOIN {{ ref('stg_sfdc__user') }} USERS
                   ON REQ.CREATEDBYID = USERS.ID
         LEFT JOIN {{ ref('stg_sfdc_locations__churn_request_locations__c') }} REQ_LOC
                   ON REQ.ID = REQ_LOC.CHURN_REQUEST__C
         LEFT JOIN {{ ref('stg_ams__location') }} AMS_LOC_1
                   ON AMS_LOC_1.SALESFORCE_ID = REQ_LOC.LOCATION__C
                       AND REQ_LOC.LOCATION__C IS NOT NULL
         LEFT JOIN {{ ref('stg_ams__account') }} AMS_ACCT
                   ON AMS_ACCT.SALESFORCE_ID = REQ.ACCOUNT__C
                       AND REQ_LOC.LOCATION__C IS NULL
         LEFT JOIN {{ ref('stg_ams__location') }} AMS_LOC_2
                   ON AMS_LOC_2.ACCOUNT_ID = AMS_ACCT.ACCOUNT_ID
                       AND REQ_LOC.LOCATION__C IS NULL
                       AND AMS_ACCT.ACCOUNT_ID IS NOT NULL
         LEFT JOIN {{ ref('stg_sfdc__account') }} SFDC_ACCT
                   ON REQ.ACCOUNT__C = SFDC_ACCT.ID
                       AND REQ_LOC.LOCATION__C IS NULL
                       AND AMS_ACCT.ACCOUNT_ID IS NULL
         LEFT JOIN {{ ref('stg_sfdc__account') }} SFDC_ACCT_2
                   ON SFDC_ACCT_2.ID = SFDC_ACCT.PARENTID
                       AND REQ_LOC.LOCATION__C IS NULL
                       AND AMS_ACCT.ACCOUNT_ID IS NULL
         LEFT JOIN {{ ref('stg_ams__account') }} AMS_ACCT_2
                   ON AMS_ACCT_2.SALESFORCE_ID = SFDC_ACCT_2.ID
                       AND REQ_LOC.LOCATION__C IS NULL
                       AND AMS_ACCT.ACCOUNT_ID IS NULL
                       AND SFDC_ACCT_2.PARENTID IS NOT NULL
         LEFT JOIN {{ ref('stg_ams__location') }} AMS_LOC_3
                   ON AMS_LOC_3.ACCOUNT_ID = AMS_ACCT_2.ACCOUNT_ID
                       AND REQ_LOC.LOCATION__C IS NULL
                       AND AMS_ACCT.ACCOUNT_ID IS NULL
                       AND SFDC_ACCT_2.PARENTID IS NOT NULL
                       AND AMS_ACCT_2.ACCOUNT_ID IS NOT NULL
         LEFT JOIN {{ ref('stg_sfdc_locations__location__c') }} SFDC_LOC
                   ON SFDC_LOC.ACCOUNT__C = REQ.ACCOUNT__C
                       AND AMS_LOC_1.LOCATION_ID IS NULL
                       AND AMS_LOC_2.LOCATION_ID IS NULL
                       AND AMS_LOC_3.LOCATION_ID IS NULL
         LEFT JOIN {{ ref('stg_ams__location') }} AMS_LOC_4
                   ON SFDC_LOC.ID = AMS_LOC_4.SALESFORCE_ID
                       AND AMS_LOC_1.LOCATION_ID IS NULL
                       AND AMS_LOC_2.LOCATION_ID IS NULL
                       AND AMS_LOC_3.LOCATION_ID IS NULL
                       AND SFDC_LOC.ID IS NOT NULL
         LEFT JOIN {{ ref('stg_sfdc_locations__location__c') }} SFDC_LOC_2
                   ON SFDC_LOC_2.ACCOUNT__C = SFDC_ACCT.PARENTID
                       AND AMS_LOC_1.LOCATION_ID IS NULL
                       AND AMS_LOC_2.LOCATION_ID IS NULL
                       AND AMS_LOC_3.LOCATION_ID IS NULL
                       AND AMS_LOC_4.LOCATION_ID IS NULL
         LEFT JOIN {{ ref('stg_ams__location') }} AMS_LOC_5
                   ON SFDC_LOC_2.ID = AMS_LOC_5.SALESFORCE_ID
                       AND AMS_LOC_1.LOCATION_ID IS NULL
                       AND AMS_LOC_2.LOCATION_ID IS NULL
                       AND AMS_LOC_3.LOCATION_ID IS NULL
                       AND AMS_LOC_4.LOCATION_ID IS NULL
                       AND SFDC_LOC_2.ID IS NOT NULL