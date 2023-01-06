--
use warehouse &{whname};
use database &{tgtdbname};
use schema &{tgtschemaname};
use role &{rolename};

--
ALTER SESSION SET TIMEZONE = 'UTC';
--
-- requests that came in the last 7 days
create or replace temporary table zenalytics.privacy.privacy_deletes as 
with location_accounts_ as (
	select distinct
        coalesce(parent_id, business_id) as root_business_id
        , business_id
  	from zenprod.crm.portal_businessprofile
)
, privacy_request_ as (
  select *
  from zenprod.privacy.privacy_request p 
  where request_type = 'REQUEST_TYPE_ERASURE'
          and created >= dateadd(day,-7,current_date() )  
)
, privacy_request_global_ as (
  select distinct NULL as business_id, contact_info, TRUE as is_global
  from privacy_request_
  where ifnull(root_business_id, '') = ''
)
, privacy_request_merchant_ as (
  select business_id, contact_info, FALSE as is_global
  from privacy_request_ p
  left join location_accounts_ a
    on p.root_business_id = a.root_business_id
  where ifnull(p.root_business_id, '') <> ''
)
select business_id, contact_info, is_global from privacy_request_global_
union
select business_id, contact_info, is_global from privacy_request_merchant_ where contact_info not in (select contact_info from privacy_request_global_);

-- Honouring MERCHANT privacy deletes
-- [Mark G.] When someone asks to not be tracked at a business anymore, we just need to update in_business_network to false to indicate that only ZENREACH should be able to see this information 
-- (unless ZENREACH delete has been requested as well)
-- [Carrie I.] updates to presence model in_business_network split between -> known_to_merchant_account and known_to_merchant_location.

-- Honouring ZENREACH privacy deletes
-- When someone asks to be forgotten by zenreach, we must either delete or remove all personal data from the sighting.  If you elect to anonymize (which I recommend), then the following fields must be set to null:
-- client mac, contact_info, contact_id, contact_method

update zenalytics.presence.finished_sightings s
set s.known_to_merchant_account = FALSE -- merchant delete --> minimum deletion
	, s.known_to_merchant_location = FALSE -- merchant delete --> minimum deletion
	, s.known_to_zenreach = case when is_global = TRUE then FALSE else s.known_to_zenreach end -- global (known_to_zenreach should always = TRUE if contact_info is not NULL)
	, s.client_mac_info = case when is_global = TRUE then NULL else s.client_mac_info end -- global
	, s.contact_info = case when is_global = TRUE then NULL else s.contact_info end -- global
    , s.contact_id = case when is_global = TRUE then NULL else s.contact_id end -- global
    , s.contact_method = case when is_global = TRUE then NULL else s.contact_method end -- global
from zenalytics.privacy.privacy_deletes pr
where pr.contact_info = s.contact_info
	  and (pr.business_id is NULL or pr.business_id = s.location_id) ;
