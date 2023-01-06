-- US ACS census info
create or replace table zenalytics.business_profiles.d_business_ACS_Census
(
BUSINESS_ID varchar,
Census_Year integer,
State_FIPS integer,
County_FIPS integer,
Place_Name varchar,
Place_FIPS integer,
Census_Tract_Code integer,
Census_Block_Code integer,
Census_Block_Group integer,
Full_FIPS integer,
Metro_Micro_Statistical_Area_Name varchar,
Metro_Micro_Statistical_Area_Code integer,
Metro_Micro_Statistical_Area_Type varchar,
Combined_Statistical_Area_Name varchar,
Combined_Statistical_Area_Code integer,
Metropolitan_Division_Area_Name varchar,
Metropolitan_Division_Area_Code integer
)
