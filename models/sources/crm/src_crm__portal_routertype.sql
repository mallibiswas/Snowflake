SELECT $1:_id::STRING                                           AS ROUTERTYPE_ID,
       $1:comment::STRING                                      AS comment,
       $1:router_version::STRING                                AS ROUTER_VERSION,
       $1:is_custom_firmware::BOOLEAN                           AS IS_CUSTOM_FIRMWARE,
       $1:known_unsupportable::BOOLEAN                          AS KNOWN_UNSUPPORTABLE,
       $1:auth_technique::STRING                                AS AUTH_TECHNIQUE,
       $1:workflow_class::STRING                                AS WORKFLOW_CLASSS,
       $1:router_make::STRING                                   AS ROUTER_MAKE,
       $1:custom_firmware_file_1::STRING                        AS CUSTOM_FIRMWARE_FILE_1,
       $1:custom_firmware_file_2::STRING                        AS CUSTOM_FIRMWARE_FILE_2,
       $1:default_username::STRING                              AS DEFAULT_USERNAME,
       $1:software_version::STRING                              AS SOFTWARE_VERSION,
       $1:wan_ifname::STRING                                    AS WAN_IFNAME,
       $1:router_model::STRING                                  AS ROUTER_MODEL,
       $1:stock_firmware_file::STRING                           AS STOCK_FIRMWARE_FILE,
       $1:non_consumer                                          AS NON_CONSUMER,
       $1:takeover_technique::STRING                            AS TAKEOVER_TECHNIQUE,
       $1:custom_routertype_id::STRING                          AS CUSTOM_ROUTERTYPE_ID,
       $1:configuration_logic_id:"$oid"::STRING                 AS CONFIGURATION_LOGIC_ID,
       replace($1:analytics_detection_logic::VARIANT, '\\', '') AS ANALYTICS_DETECTION_LOGIC,
       $1:override_custom_logic_id:"$oid"::STRING               AS OVERRIDE_CUSTOM_LOGIC_ID,
       $1:detection_logic::VARIANT                              AS DETECTION_LOGIC,
       current_date                                     AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_routertype.json') }}