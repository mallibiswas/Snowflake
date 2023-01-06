
use role &{rolename};
use warehouse &{whname};
use database &{dbname};

-------------------------------------------------------------------------------
--- Build Secure Views for ABI_SHARE
-------------------------------------------------------------------------------

show grants on schema zenshare.abi;

create or replace secure view zenshare.abi.businessprofile_hierarchy
as
select * from zenshare.main.businessprofile_hierarchy
where parent_id = '591f111d4a4f9f000c17a583';
alter view BUSINESSPROFILE_HIERARCHY set secure;

create or replace secure view zenshare.abi.customer_profile
as
select * from zenshare.main.customer_profile
where parent_id = '591f111d4a4f9f000c17a583';
alter view CUSTOMER_PROFILE set secure;

----- Subset of visits_smry as a source for visits smry fact
create or replace secure view zenshare.abi.visits_smry
as
select visit_date,
business_id,
contact_id,
contact_method,
visit_duration_mins,
visit_count,
first_visit,
visit_day,
first_start_time as create_dttm,
last_end_time as update_dttm,
asof_date
from zenshare.main.visits_smry
where business_id in (select business_id from zenshare.abi.businessprofile_hierarchy where parent_id = '591f111d4a4f9f000c17a583');
alter view VISITS_SMRY set secure;
