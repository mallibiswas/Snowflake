WITH AMS_V3_SUBSCRIPTIONS AS (
    SELECT ACCOUNT_ID,
           S.ID AS SUBSCRIPTION_ID,
           S.RECURLY_SUBSCRIPTION_ID
    FROM {{ ref('stg_ams_account__subscription') }} S,
         {{ ref('stg_ams_account__recurly_subscription') }} RS
    WHERE S.RECURLY_SUBSCRIPTION_ID = RS.RECURLY_SUBSCRIPTION_ID
),
     AMS_V3_OPPORTUNITIES AS (
         SELECT O.ACCOUNT_ID,
                A.SUBSCRIPTION_ID,
                SO.OPPORTUNITYID AS SALESFORCE_OPPORTUNITY_ID
         FROM {{ ref('stg_ams_account__orders' ) }} O,
              {{ ref('stg_sfdc__order') }} SO,
              {{ ref('stg_ams_account__order_item') }} OI,
              {{ ref('stg_ams_account__asset') }} A
         WHERE SO.ID = O.SALESFORCE_ORDER_ID
           AND O.ID = OI.ORDER_ID
           AND A.ID = OI.ASSET_ID
           AND ITEM_TYPE = 'subscription'
     ),

     AMS_V2_OPPORTUNITIES AS (SELECT to_varchar(ACCOUNT_ID) AS ACCOUNT_ID,
                                     SALESFORCE_OPPORTUNITY_ID,
                                     RECURLY_SUBSCRIPTION_ID
                              FROM {{ ref('stg_ams__subscriptions_v2') }}),

     AMS_V2_MIGRATIONS AS (SELECT to_varchar(ACCOUNT_ID) AS ACCOUNT_ID,
                                  SALESFORCE_OPPORTUNITY_ID,
                                  RECURLY_SUBSCRIPTION_ID
                           FROM {{ ref('stg_ams__subscriptions_v2_pre_migration') }}),

     AMS_V1_OPPORTUNITIES AS (SELECT to_varchar(S.ACCOUNT_ID) AS ACCOUNT_ID,
                                     SALESFORCE_OPPORTUNITY_ID,
                                     RS.RECURLY_SUBSCRIPTION_ID
                              FROM {{ ref('stg_ams__contract') }} C,
                                   {{ ref('stg_ams__subscription') }} S,
                                   {{ ref('stg_recurly__subscriptions') }} RS
                              WHERE C.SUBSCRIPTION_ID = S.SUBSCRIPTION_ID
                                AND S.RECURLY_SUBSCRIPTION_TOKEN = RS.RECURLY_SUBSCRIPTION_ID)
        ,
     _OPPORTUNITY_SUBSCRIPTION_LOOKUP_ AS (
         SELECT 'v1'                                     AS VERSION,
                ACCOUNT_ID,
                substr(SALESFORCE_OPPORTUNITY_ID, 1, 15) AS SALESFORCE_OPPORTUNITY_ID,
                RECURLY_SUBSCRIPTION_ID
         FROM AMS_V1_OPPORTUNITIES
         UNION
         SELECT 'v2'                                     AS VERSION,
                ACCOUNT_ID,
                substr(SALESFORCE_OPPORTUNITY_ID, 1, 15) AS SALESFORCE_OPPORTUNITY_ID,
                RECURLY_SUBSCRIPTION_ID
         FROM AMS_V2_OPPORTUNITIES
         UNION
         SELECT 'v2'                                     AS VERSION,
                ACCOUNT_ID,
                substr(SALESFORCE_OPPORTUNITY_ID, 1, 15) AS SALESFORCE_OPPORTUNITY_ID,
                RECURLY_SUBSCRIPTION_ID
         FROM AMS_V2_MIGRATIONS
         UNION
         SELECT 'v3'                                     AS VERSION,
                V3S.ACCOUNT_ID,
                substr(SALESFORCE_OPPORTUNITY_ID, 1, 15) AS SALESFORCE_OPPORTUNITY_ID,
                RECURLY_SUBSCRIPTION_ID
         FROM AMS_V3_OPPORTUNITIES V3O,
              AMS_V3_SUBSCRIPTIONS V3S
         WHERE V3O.SUBSCRIPTION_ID = V3S.SUBSCRIPTION_ID)
SELECT O.*,
       so.TYPE      AS OPPORTUNITY_TYPE,
       current_date AS ASOF_DATE
FROM _OPPORTUNITY_SUBSCRIPTION_LOOKUP_ O
         LEFT JOIN {{ ref('stg_sfdc__opportunity') }} so
ON o.salesforce_opportunity_id = SUBSTR(so.id,1,15)


