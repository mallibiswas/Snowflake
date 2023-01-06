create or replace view zenprod.privacy.privacy_deletes
as
WITH 
all_contacts as (select contact_info, to_date(created) as created_date from zenprod.privacy.privacy_request),
merchant_deletes as (select contact_info from zenprod.privacy.privacy_request where ifnull(root_business_id, '') <> ''),
global_deletes as (select contact_info from zenprod.privacy.privacy_request where ifnull(root_business_id, '') = '')
select distinct 
    a.contact_info, 
    created_date,
    case when b.contact_info is not null then True else False end as is_merchant, 
    case when c.contact_info is not null then True else False end as is_global
from all_contacts a
left join global_deletes b on a.contact_info = b.contact_info
left join merchant_deletes c on a.contact_info = c.contact_info;