-------------------------------------------------------
--------------  INSERT INTO.&{schemaname}.MAIL_HOOK -------------
-------------------------------------------------------

use warehouse &{whname};
use database &{dbname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

SET MIN_ROWID = (SELECT MAX(source_rowid) FROM &{dbname}.&{schemaname}.MAIL_HOOK);

CREATE OR REPLACE VIEW &{dbname}._staging._MAIL_HOOK_VW_ comment='View for loading mail_hook' 
AS 
    SELECT  rowid,
            envelope,
            source, 
            envelope:recipient::string as email, 
            envelope:timestamp:seconds::integer as timestamp, 
            type::string as type, 
            f.value:key::string as cols, 
            f.value:value::string as row_values
    FROM &{dbname}.&{stageschema}._MAIL_HOOK_0_ p,   lateral flatten(input => p.arguments) f
    WHERE rowid > $MIN_ROWID
;

INSERT INTO &{dbname}.&{schemaname}.MAIL_HOOK
	(source_rowid,
	blast,
	business_id,
	comments_updated,
	created,
	domain,
	email,
	envelope,
	event_timestamp,
	h,
	is_test,
	list_unsubscribe_email,
	list_unsubscribe_url,
	mergedblast,
	message_id,
	message_type,
	messagelog_id,
	pool,
	portal_session_id,
	rating,
	rating_updated,
	source,
	timestamp,
	trigger_id,
	type,
	userprofile_id,
	location_ids, 
	mac, 
	uuid)
SELECT  
	rowid as source_rowid,
	blast,
	business_id,
	comments_updated,
	created,
	domain,
	email,
	envelope,
	event_timestamp,
	h,
	is_test,
	list_unsubscribe_email,
	list_unsubscribe_url,
	mergedblast,
	message_id,
	message_type,
	messagelog_id,
	pool,
	portal_session_id,
	rating,
	rating_updated,
	source,
	timestamp,
	trigger_id,
	type,
	userprofile_id,
	split(location_ids,',')::variant as location_ids, 
	mac, 
	uuid
FROM &{dbname}.&{stageschema}._MAIL_HOOK_VW_ 
PIVOT(MAX(row_values) 
FOR cols IN ('blast','business','created','domain','event-timestamp','h','list_unsubscribe_email','list_unsubscribe_url', 'mergedblast','message','message_type','messagelog','pool','portal_session_id','rating','rating_updated','comments_updated', 'trigger','userprofile','is-test','location_ids','mac','uuid'))
AS pv (rowid, envelope, source, email, timestamp, type, blast, business_id, created, domain, event_timestamp, h, list_unsubscribe_email, list_unsubscribe_url, mergedblast, message_id, message_type, messagelog_id, pool, portal_session_id, rating, rating_updated, comments_updated, trigger_id, userprofile_id, is_test, location_ids, mac, uuid)
ORDER BY rowid, envelope, source, email, timestamp, type;

