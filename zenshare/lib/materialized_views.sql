
use role &{rolename};
use warehouse &{whname};
use database &{dbname};

-------------------------------------------------------------------------------
----- Customer view as a source for the Customer Dimension
-------------------------------------------------------------------------------

create or replace materialized view zenshare.main.d_visitor
comment='visitor dimension is a subset of analytics customer'
as 
select  customer_id, 
	business_id,
	email,
        age, 
        city, 
        state, 
        gender, 
        income 
from zenalytics.crm.analytics_customer;

-------------------------------------------------------------------------------
----- Customer view as a source for Business Profile
-------------------------------------------------------------------------------

create or replace materialized view zenshare.main.d_businessprofile
comment='copy of business profile with subset of shareable attributes'
as
select
  business_id,
  name as business_name,
  shortname,
  address as business_address,
  announcements,
  created_date,
  facebook_like_url,
  last_crm_req,
  last_probe_upload,
  public_ssid,
  time_zone,
  website_url,
  yelp_url
from zenalytics.crm.portal_businessprofile;


-------------------------------------------------------------------------------
----- Business Profile Hierarchy
-------------------------------------------------------------------------------

create or replace materialized view zenshare.main.d_businessprofile_hierarchy
comment='copy of business profile hierarchy'
as 
select * from zenalytics.crm.businessprofile_hierarchy;

-------------------------------------------------------------------------------
----- Analytics Customer 
-------------------------------------------------------------------------------

create or replace materialized view zenshare.main.analytics_customer
comment='copy of Anlytics Customer'
as 
select * from zenalytics.crm.analytics_customer;


-------------------------------------------------------------------------------
----- Portal Userprofile 
-------------------------------------------------------------------------------

create or replace materialized view zenshare.main.portal_userprofile
comment='copy of Portal Userprofile'
as 
select * from zenalytics.crm.portal_userprofile;



