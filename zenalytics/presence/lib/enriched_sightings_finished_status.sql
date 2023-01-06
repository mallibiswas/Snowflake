--------------------------------------------------------------------------------
---------  create table presence.enriched_sightings_finished_status -------------
--------------------------------------------------------------------------------

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{tgtdbname};
use role &{rolename};

SET MIN_UNIX_TS = (select max(end_time) from &{tgtdbname}.&{tgtschemaname}.&{tgttablename});

SET MAX_UNIX_TS = (select DATE_PART(EPOCH_MILLISECOND, to_timestamp_ntz(current_date())));

SELECT concat('Inserting from ts: ',$MIN_UNIX_TS,' (',to_timestamp($MIN_UNIX_TS,3),') to :',$MAX_UNIX_TS,' (',to_timestamp($MAX_UNIX_TS,3),')');

-- SELECT concat('Inserting from ts: ',$MIN_UNIX_TS,'=',to_timestamp($MIN_UNIX_TS,3));

INSERT INTO &{tgtdbname}.&{tgtschemaname}.&{tgttablename}
(CLIENT_MAC_ANONYMIZED,
CLIENT_MAC,
ASSIGNMENT,
CONTACT_CREATED_DATE,
CONTACT_ID,
CONTACT_INFO,
CONTACT_METHOD,
IN_BUSINESS_NETWORK,
BUSINESS_ID,
START_DATE,
START_TIME,
END_TIME,
IS_WALK_IN,
SOURCE,
BLIP_COUNT,
MAX_RSSI,
MIN_RSSI,
AVG_RSSI,
STATUS,
ASOF_DATE)
SELECT
CLIENT_MAC_ANONYMIZED,
CLIENT_MAC,
ASSIGNMENT,
CONTACT_CREATED_DATE,
CONTACT_ID,
CONTACT_INFO,
CONTACT_METHOD,
IN_BUSINESS_NETWORK,
BUSINESS_ID,
START_DATE,
START_TIME,
END_TIME,
IS_WALK_IN,
SOURCE,
BLIP_COUNT,
MAX_RSSI,
MIN_RSSI,
AVG_RSSI,
STATUS,
current_date() as ASOF_DATE
FROM &{srcdbname}.&{srcschemaname}.&{srctablename}
WHERE status = 'FINISHED'
AND end_time > $MIN_UNIX_TS 
AND end_time <= $MAX_UNIX_TS
;
