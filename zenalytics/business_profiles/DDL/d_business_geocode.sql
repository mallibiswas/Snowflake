--------------------------------------------------------------------
-- table for storing geocoded addresses for US and Canadian Businesses
--------------------------------------------------------------------

create or replace table zenalytics.business_profiles.d_business_geocode
(BUSINESS_ID varchar, 
 Latitude	number(28,10),
 Longitude	number(28,10),
 Accuracy_Score	number(28,10),
 Accuracy_Type  varchar,	
 Number	varchar,
 Street	 varchar,
 City	 varchar,
 State	 varchar,
 County	 varchar,
 Zip	 varchar,
 Country	 varchar,
 Source	 varchar,
 Timezone_Name  varchar,	
 Timezone_Abbreviation	 varchar,
 Timezone_UTC_Offset  number(28,10),	
 Timezone_Observes_Daylight_Savings  varchar);

