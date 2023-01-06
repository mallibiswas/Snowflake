with location_accounts_ as (
	select distinct
        parent_id as root_business_id
        , business_id
  	from {{ ref('stg_crm__businessprofile_hierarchy') }}
)
, privacy_request_ as (
  select *
  from {{ source('PRIVACY', 'PRIVACY_REQUEST') }} p
  where request_type = 'REQUEST_TYPE_ERASURE'
          and created >= dateadd(day,-7,current_date() )
)
, privacy_request_global_ as (
  select distinct NULL as business_id, contact_info, TRUE as is_global, FALSE as is_merchant
  from privacy_request_
  where ifnull(root_business_id, '') = ''
)
, privacy_request_merchant_ as (
  select business_id, contact_info, FALSE as is_global, TRUE as is_merchant
  from privacy_request_ p
  left join location_accounts_ a
    on p.root_business_id = a.root_business_id
  where ifnull(p.root_business_id, '') <> ''
)
select business_id, contact_info, is_global, is_merchant from privacy_request_global_
union
select business_id, contact_info, is_global, is_merchant from privacy_request_merchant_ where contact_info not in (select contact_info from privacy_request_global_)