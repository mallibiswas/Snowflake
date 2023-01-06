-------------------------------------------------------------------------
-- canadian census reference
-------------------------------------------------------------------------

create or replace table zenalytics.business_profiles.d_business_statcan_census
(
business_id varchar,
Division_id varchar,
Division_Name varchar,
Division_Type varchar,
Division_Type_Description varchar,
Consolidated_Subdivision_id varchar,
Consolidated_Subdivision_Name varchar,
Subdivision_id  varchar,
Subdivision_Name  varchar,
Subdivision_Type varchar, 
Subdivision_Type_Description varchar,
Economic_Region varchar,
Statistical_Area_Code varchar,
Statistical_Area_Code_Description varchar,
Statistical_Area_Type varchar,
Statistical_Area_Type_Description varchar,
CMA_CA_id varchar,
CMA_CA_Name varchar,
CMA_CA_Type varchar,
CMA_CA_Type_Description varchar,
Census_Tract varchar,
Census_Year integer)
;
