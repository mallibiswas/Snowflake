SELECT $1:_id:"$oid"::STRING                                  AS USERTUTORIALCOMPLETION_ID,
       $1:completed:composer_blast_edit:"$date"::DATETIME     AS COMPOSER_BLAST_EDIT,
       $1:completed:composer_blast_template:"$date"::DATETIME AS COMPOSER_BLAST_TEMPLATE,
       $1:completed:composer_sm_edit:"$date"::DATETIME        AS COMPOSER_SM_EDIT,
       $1:completed:composer_sm_template:"$date"::DATETIME    AS COMPOSER_SM_TEMPLATE,
       $1:completed:contacts:"$date"::DATETIME                AS CONTACTS,
       $1:completed:email_campaign:"$date"::DATETIME          AS EMAIL_CAMPAIGN,
       $1:completed:insights:"$date"::DATETIME                AS COMPLETED,
       $1:completed:smart_message:"$date"::DATETIME           AS SMART_MESSAGE,
       $1:completed:"tutorial-index":"$date"::DATETIME        AS "TUTORIAL-INDEX",
       $1:completed:hotspot:"$date"::DATETIME                 AS HOTSPOT,
       $1:email::STRING                                      AS EMAIL,
       $1:skipped_all::BOOLEAN                                AS SKIPPED_ALL,
       current_date                                   AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/models_usertutorialcompletion.json') }}