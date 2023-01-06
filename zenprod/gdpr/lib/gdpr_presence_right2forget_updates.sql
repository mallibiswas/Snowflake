--
use warehouse &{whname};
use database &{dbname};
use schema &{tgtschemaname};
use role &{rolename};

--
ALTER SESSION SET TIMEZONE = 'UTC';
--
-- requests that came in the last 7 days
create or replace temporary table &{dbname}.&{stageschemaname}.&{reftablename} as select * from &{dbname}.&{refschemaname}.&{reftablename} where created_date >= dateadd(day,-7,current_date);

-- Honouring MERCHANT privacy deletes
-- [Mark G.] When someone asks to not be tracked at a business anymore, we just need to update in_business_network to false to indicate that only ZENREACH should be able to see this information 
-- (unless ZENREACH delete has been requested as well)

-- Honouring ZENREACH privacy deletes
-- When someone asks to be forgotten by zenreach, we must either delete or remove all personal data from the sighting.  If you elect to anonymize (which I recommend), then the following fields must be set to null:
-- client mac, contact_info, contact_id, contact_method

update &{dbname}.&{tgtschemaname}.&{tgttablename} es
set es.in_business_network = is_global, -- merchant delete only if is_global = False
    es.contact_id = case when is_global = True then Null end, -- global
    es.contact_method = case when is_global = True then Null end, -- global
    es.contact_info = case when is_global = True then Null end -- global
from  &{dbname}.&{stageschemaname}.&{reftablename} pr
where pr.contact_info = es.contact_info;
