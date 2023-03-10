SELECT $1:_id:"$oid"::STRING                         AS DEFAULTTRIGGER_ID,
       $1:business_id:"$oid"::STRING                 AS BUSINESS_ID,
       $1:message_id:"$oid"::STRING                  AS MESSAGE_ID,
       $1:demographic_rule::STRING                   AS DEMOGRAPHIC_RULE,
       $1:description::STRING                        AS DESCRIPTION,
       $1:enabled::BOOLEAN                           AS ENABLED,
       $1:is_recurring::BOOLEAN                      AS IS_RECURRING,
       $1:linked::BOOLEAN                            AS LINKED,
       $1:parent_id::STRING                          AS PARENT_ID,
       replace($1:parameters::VARIANT, '\\', '')    AS PARAMETERS,
       replace($1:proximity_rule::VARIANT, '\\', '') AS PROXIMITY_RULE,
       replace($1:purchase_rule::VARIANT, '\\', '')  AS PURCHASE_RULE,
       replace($1:rule::VARIANT, '\\', '')           AS RULE,
       $1:timebox::INTEGER                           AS TIMEBOX,
       $1:title::STRING                              AS TITLE,
       current_date                          AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_defaulttrigger.json') }}
