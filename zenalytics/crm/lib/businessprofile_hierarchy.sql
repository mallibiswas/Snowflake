-----------------------------------------------------------------------
---------------------  BUSINESSPROFILE HIERARCHY ----------------------
-----------------------------------------------------------------------

use role &{rolename};
use database &{dbname};
use schema &{schemaname};
use warehouse &{whname};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

create or replace table &{dbname}.&{stageschemaname}.businessprofile_hierarchy 
as select *, current_date as asof_date 
from &{dbname}.&{schemaname}.businessprofile_hierarchy_vw;
 
alter table &{dbname}.&{stageschemaname}.businessprofile_hierarchy swap with &{dbname}.&{schemaname}.businessprofile_hierarchy;
