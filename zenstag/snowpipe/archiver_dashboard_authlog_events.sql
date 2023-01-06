CREATE OR REPLACE stage ZENSTAG.CRM.ARCHIVER_DASHBOARD_AUTHLOG_EVENT_S3_STAGE
    file_format = ( TYPE = JSON )
    url = 's3://zs-uw2-platform-kafka-archives/archiver/dashboard_authlog_event_byid_0/'
    credentials = ( aws_role = 'arn:aws:iam::255794285552:role/zs-uw2-archiver-dashboard-authlog-event-snowflake-stage');

create table if not exists ZENSTAG.CRM.ARCHIVER_AUTHLOG_EVENT (
  ID VARCHAR(16777216),
  USERNAME VARCHAR(16777216),
  TIMESTAMP TIMESTAMP,
  LOGIN_SUCCESS BOOLEAN,
  ACCOUNT_KNOWN BOOLEAN,
  ACCOUNT_LOCK BOOLEAN,
  LAST_SUCCESSFUL_LOGIN TIMESTAMP,
  REMOTE_IP VARCHAR(16777216),
  SERVICE_IP VARCHAR(16777216)
)

create pipe if not exists ZENSTAG.CRM.ARCHIVER_AUTHLOG_EVENT_SNOWPIPE auto_ingest=true as
copy into ZENSTAG.CRM.ARCHIVER_AUTHLOG_EVENT
FROM
    (
      SELECT
        $1:id::string as id,
        $1:username::string as username,

        TO_TIMESTAMP_NTZ($1:timestamp) as timestamp,

        $1:login_success::boolean as login_success,
        $1:account_known::boolean as account_known,
        $1:account_lock::boolean as account_lock,

        TO_TIMESTAMP_NTZ($1:last_successful_login) as last_successful_login,

        $1:remote_ip::string as remote_ip,
        $1:service_ip::string as service_ip
      FROM @ZENSTAG.CRM.ARCHIVER_DASHBOARD_AUTHLOG_EVENT_S3_STAGE
    )
on_error = 'continue';
