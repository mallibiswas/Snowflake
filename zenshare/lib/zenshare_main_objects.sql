
use role &{rolename};
use warehouse &{whname};
use database &{targetdb};
use schema &{targetschema};

-------------------------------------------------------------------------------
--- Copy/Clone tables to ZENSHARE.MAIN
-------------------------------------------------------------------------------


----- Business Profile Hierarchy
create or replace table &{targetdb}.&{targetschema}.businessprofile_hierarchy clone &{analyticsdb}.&{analyticsschema}.businessprofile_hierarchy;

----- Portal Business Profile
create or replace table &{targetdb}.&{targetschema}.portal_businessprofile clone &{proddb}.&{prodschema}.portal_businessprofile;

----- Analytics Customer
create or replace table &{targetdb}.&{targetschema}.analytics_customer clone &{proddb}.&{prodschema}.analytics_customer;

----- Portal Use Profile
create or replace table &{targetdb}.&{targetschema}.portal_userprofile clone &{proddb}.&{prodschema}.portal_userprofile;

----- Portal Business Relationship
create or replace table &{targetdb}.&{targetschema}.portal_businessrelationship clone &{proddb}.&{prodschema}.portal_businessrelationship;

----- Portal accessdeviceownership
create or replace table &{targetdb}.&{targetschema}.portal_accessdeviceownership clone &{proddb}.&{prodschema}.portal_accessdeviceownership;

----- Visits Smry
create or replace table &{targetdb}.&{targetschema}.visits_smry clone &{sourcedb}.&{sourceschema}.visits_smry;
