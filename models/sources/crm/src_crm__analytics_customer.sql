SELECT  GET_PATH($1,'_id:$oid')::string                  AS customer_id
       ,GET_PATH($1,'business_id:$oid')::string          AS business_id
       ,$1:phone::string                                 AS phone
       ,$1:age::string                                   AS age
       ,$1:city::string                                  AS city
       ,$1:contact_allowed::boolean                      AS contact_allowed
       ,$1:default_pic_url::string                       AS default_pic_url
       ,$1:email::string                                 AS email
       ,$1:email_is_valid::boolean                       AS email_is_valid
       ,$1:emails::variant                               AS emails
       ,$1:fullname::string                              AS fullname
       ,$1:gender::string                                AS gender
       ,$1:income::string                                AS income
       ,TO_TIMESTAMP_NTZ(TO_NUMBER(SUBSTR($1:_id:"$oid", 1, 8), 'XXXXXXXX'))::datetime           AS created
       ,GET_PATH($1,'first_seen:$date')::timestamp       AS first_seen
       ,GET_PATH($1,'last_seen:$date')::timestamp        AS last_seen
       ,GET_PATH($1,'last_updated:$date')::timestamp     AS last_updated
       ,GET_PATH($1,'server_last_seen:$date')::timestamp AS server_last_seen
       ,$1:location::string                              AS location
       ,$1:state::string                                 AS state
       ,$1:zip_code::string                              AS zip_code
       ,$1:tags::variant                                 AS tags
       ,$1:macs::variant                                 AS macs
       ,$1:facebook_app_user_ids::variant                AS facebook_app_user_ids
       ,$1:messages_sent::integer                        AS messages_sent
       ,{{ parse_json_integer('$1:birthday_day') }}::variant      AS birthday_day
       ,{{ parse_json_integer('$1:birthday_month') }}::variant    AS birthday_month
       ,$1:non_customer::boolean                         AS non_customer
       ,$1:non_employee::boolean                         AS non_employee
       ,$1:offers_redeemed::integer                      AS offers_redeemed
       ,$1:offers_sent::integer                          AS offers_sent
       ,$1:purchase_count::variant                       AS purchase_count
       ,$1:validation_reason::string                     AS validation_reason
       ,{{ parse_json_integer('$1:visit_count') }}       AS visit_count
       ,current_timestamp()                              AS asof_date
FROM {{ most_recent_s3_file_name ('_STAGE', 'S3_MONGO_STAGE', '.*/analytics_customer.json') }}