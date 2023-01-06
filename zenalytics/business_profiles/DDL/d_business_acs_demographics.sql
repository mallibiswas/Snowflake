-----------------------------------------------------------------------
-- Demographic data for business locations from ACS / 2019 surveys
-----------------------------------------------------------------------

create or replace table zenalytics.business_profiles.d_business_ACS_Demographics
(
business_id varchar,
Median_age_Total_Value Number(28,10),
Median_age_Total_Margin_of_error Number(28,10),
Median_age_Male_Value Number(28,10),
Median_age_Male_Margin_of_error Number(28,10),
Median_age_Female_Value Number(28,10),
Median_age_Female_Margin_of_error Number(28,10),
Population_by_age_range_Total_Value Number(28,10),
Population_by_age_range_Total_Margin_of_error Number(28,10),
Population_by_age_range_Male_Value Number(28,10),
Population_by_age_range_Male_Margin_of_error Number(28,10),
Population_by_age_range_Male_Percentage Number(28,10),
Population_by_age_range_Male_Under_5_years_Value Number(28,10),
Population_by_age_range_Male_Under_5_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_Under_5_years_Percentage Number(28,10),
Population_by_age_range_Male_5_to_9_years_Value Number(28,10),
Population_by_age_range_Male_5_to_9_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_5_to_9_years_Percentage Number(28,10),
Population_by_age_range_Male_10_to_14_years_Value Number(28,10),
Population_by_age_range_Male_10_to_14_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_10_to_14_years_Percentage Number(28,10),
Population_by_age_range_Male_15_to_17_years_Value Number(28,10),
Population_by_age_range_Male_15_to_17_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_15_to_17_years_Percentage Number(28,10),
Population_by_age_range_Male_18_and_19_years_Value Number(28,10),
Population_by_age_range_Male_18_and_19_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_18_and_19_years_Percentage Number(28,10),
Population_by_age_range_Male_20_years_Value Number(28,10),
Population_by_age_range_Male_20_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_20_years_Percentage Number(28,10),
Population_by_age_range_Male_21_years_Value Number(28,10),
Population_by_age_range_Male_21_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_21_years_Percentage Number(28,10),
Population_by_age_range_Male_22_to_24_years_Value Number(28,10),
Population_by_age_range_Male_22_to_24_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_22_to_24_years_Percentage Number(28,10),
Population_by_age_range_Male_25_to_29_years_Value Number(28,10),
Population_by_age_range_Male_25_to_29_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_25_to_29_years_Percentage Number(28,10),
Population_by_age_range_Male_30_to_34_years_Value Number(28,10),
Population_by_age_range_Male_30_to_34_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_30_to_34_years_Percentage Number(28,10),
Population_by_age_range_Male_35_to_39_years_Value Number(28,10),
Population_by_age_range_Male_35_to_39_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_35_to_39_years_Percentage Number(28,10),
Population_by_age_range_Male_40_to_44_years_Value Number(28,10),
Population_by_age_range_Male_40_to_44_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_40_to_44_years_Percentage Number(28,10),
Population_by_age_range_Male_45_to_49_years_Value Number(28,10),
Population_by_age_range_Male_45_to_49_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_45_to_49_years_Percentage Number(28,10),
Population_by_age_range_Male_50_to_54_years_Value Number(28,10),
Population_by_age_range_Male_50_to_54_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_50_to_54_years_Percentage Number(28,10),
Population_by_age_range_Male_55_to_59_years_Value Number(28,10),
Population_by_age_range_Male_55_to_59_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_55_to_59_years_Percentage Number(28,10),
Population_by_age_range_Male_60_and_61_years_Value Number(28,10),
Population_by_age_range_Male_60_and_61_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_60_and_61_years_Percentage Number(28,10),
Population_by_age_range_Male_62_to_64_years_Value Number(28,10),
Population_by_age_range_Male_62_to_64_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_62_to_64_years_Percentage Number(28,10),
Population_by_age_range_Male_65_and_66_years_Value Number(28,10),
Population_by_age_range_Male_65_and_66_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_65_and_66_years_Percentage Number(28,10),
Population_by_age_range_Male_67_to_69_years_Value Number(28,10),
Population_by_age_range_Male_67_to_69_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_67_to_69_years_Percentage Number(28,10),
Population_by_age_range_Male_70_to_74_years_Value Number(28,10),
Population_by_age_range_Male_70_to_74_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_70_to_74_years_Percentage Number(28,10),
Population_by_age_range_Male_75_to_79_years_Value Number(28,10),
Population_by_age_range_Male_75_to_79_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_75_to_79_years_Percentage Number(28,10),
Population_by_age_range_Male_80_to_84_years_Value Number(28,10),
Population_by_age_range_Male_80_to_84_years_Margin_of_error Number(28,10),
Population_by_age_range_Male_80_to_84_years_Percentage Number(28,10),
Population_by_age_range_Male_85_years_and_over_Value Number(28,10),
Population_by_age_range_Male_85_years_and_over_Margin_of_error Number(28,10),
Population_by_age_range_Male_85_years_and_over_Percentage Number(28,10),
Population_by_age_range_Female_Value Number(28,10),
Population_by_age_range_Female_Margin_of_error Number(28,10),
Population_by_age_range_Female_Percentage Number(28,10),
Population_by_age_range_Female_Under_5_years_Value Number(28,10),
Population_by_age_range_Female_Under_5_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_Under_5_years_Percentage Number(28,10),
Population_by_age_range_Female_5_to_9_years_Value Number(28,10),
Population_by_age_range_Female_5_to_9_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_5_to_9_years_Percentage Number(28,10),
Population_by_age_range_Female_10_to_14_years_Value Number(28,10),
Population_by_age_range_Female_10_to_14_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_10_to_14_years_Percentage Number(28,10),
Population_by_age_range_Female_15_to_17_years_Value Number(28,10),
Population_by_age_range_Female_15_to_17_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_15_to_17_years_Percentage Number(28,10),
Population_by_age_range_Female_18_and_19_years_Value Number(28,10),
Population_by_age_range_Female_18_and_19_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_18_and_19_years_Percentage Number(28,10),
Population_by_age_range_Female_20_years_Value Number(28,10),
Population_by_age_range_Female_20_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_20_years_Percentage Number(28,10),
Population_by_age_range_Female_21_years_Value Number(28,10),
Population_by_age_range_Female_21_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_21_years_Percentage Number(28,10),
Population_by_age_range_Female_22_to_24_years_Value Number(28,10),
Population_by_age_range_Female_22_to_24_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_22_to_24_years_Percentage Number(28,10),
Population_by_age_range_Female_25_to_29_years_Value Number(28,10),
Population_by_age_range_Female_25_to_29_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_25_to_29_years_Percentage Number(28,10),
Population_by_age_range_Female_30_to_34_years_Value Number(28,10),
Population_by_age_range_Female_30_to_34_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_30_to_34_years_Percentage Number(28,10),
Population_by_age_range_Female_35_to_39_years_Value Number(28,10),
Population_by_age_range_Female_35_to_39_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_35_to_39_years_Percentage Number(28,10),
Population_by_age_range_Female_40_to_44_years_Value Number(28,10),
Population_by_age_range_Female_40_to_44_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_40_to_44_years_Percentage Number(28,10),
Population_by_age_range_Female_45_to_49_years_Value Number(28,10),
Population_by_age_range_Female_45_to_49_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_45_to_49_years_Percentage Number(28,10),
Population_by_age_range_Female_50_to_54_years_Value Number(28,10),
Population_by_age_range_Female_50_to_54_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_50_to_54_years_Percentage Number(28,10),
Population_by_age_range_Female_55_to_59_years_Value Number(28,10),
Population_by_age_range_Female_55_to_59_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_55_to_59_years_Percentage Number(28,10),
Population_by_age_range_Female_60_and_61_years_Value Number(28,10),
Population_by_age_range_Female_60_and_61_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_60_and_61_years_Percentage Number(28,10),
Population_by_age_range_Female_62_to_64_years_Value Number(28,10),
Population_by_age_range_Female_62_to_64_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_62_to_64_years_Percentage Number(28,10),
Population_by_age_range_Female_65_and_66_years_Value Number(28,10),
Population_by_age_range_Female_65_and_66_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_65_and_66_years_Percentage Number(28,10),
Population_by_age_range_Female_67_to_69_years_Value Number(28,10),
Population_by_age_range_Female_67_to_69_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_67_to_69_years_Percentage Number(28,10),
Population_by_age_range_Female_70_to_74_years_Value Number(28,10),
Population_by_age_range_Female_70_to_74_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_70_to_74_years_Percentage Number(28,10),
Population_by_age_range_Female_75_to_79_years_Value Number(28,10),
Population_by_age_range_Female_75_to_79_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_75_to_79_years_Percentage Number(28,10),
Population_by_age_range_Female_80_to_84_years_Value Number(28,10),
Population_by_age_range_Female_80_to_84_years_Margin_of_error Number(28,10),
Population_by_age_range_Female_80_to_84_years_Percentage Number(28,10),
Population_by_age_range_Female_85_years_and_over_Value Number(28,10),
Population_by_age_range_Female_85_years_and_over_Margin_of_error Number(28,10),
Population_by_age_range_Female_85_years_and_over_Percentage Number(28,10),
Sex_Total_Value Number(28,10),
Sex_Total_Margin_of_error Number(28,10),
Sex_Male_Value Number(28,10),
Sex_Male_Margin_of_error Number(28,10),
Sex_Male_Percentage Number(28,10),
Sex_Female_Value Number(28,10),
Sex_Female_Margin_of_error Number(28,10),
Sex_Female_Percentage Number(28,10),
Race_and_ethnicity_Total_Value Number(28,10),
Race_and_ethnicity_Total_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Percentage Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_White_alone_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_White_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_White_alone_Percentage Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Black_or_African_American_alone_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Black_or_African_American_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Black_or_African_American_alone_Percentage Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone_Percentage Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Asian_alone_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Asian_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Asian_alone_Percentage Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Native_Hawaiian_and_Other_Pacific_Islander_alone_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Native_Hawaiian_and_Other_Pacific_Islander_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Native_Hawaiian_and_Other_Pacific_Islander_alone_Percentage Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Some_other_race_alone_Value Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Some_other_race_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Not_Hispanic_or_Latino_Some_other_race_alone_Percentage Number(28,10),
Race_and_ethnicity_Two_or_more_races_Value Number(28,10),
Race_and_ethnicity_Two_or_more_races_Margin_of_error Number(28,10),
Race_and_ethnicity_Two_or_more_races_Percentage Number(28,10),
Race_and_ethnicity_Two_or_more_races_Two_races_including_Some_other_race_Value Number(28,10),
Race_and_ethnicity_Two_or_more_races_Two_races_including_Some_other_race_Margin_of_error Number(28,10),
Race_and_ethnicity_Two_or_more_races_Two_races_including_Some_other_race_Percentage Number(28,10),
Race_and_ethnicity_Two_or_more_races_Two_races_excluding_Some_other_race_and_three_or_more_races_Value Number(28,10),
Race_and_ethnicity_Two_or_more_races_Two_races_excluding_Some_other_race_and_three_or_more_races_Margin_of_error Number(28,10),
Race_and_ethnicity_Two_or_more_races_Two_races_excluding_Some_other_race_and_three_or_more_races_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_White_alone_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_White_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_White_alone_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Black_or_African_American_alone_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Black_or_African_American_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Black_or_African_American_alone_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Asian_alone_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Asian_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Asian_alone_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Native_Hawaiian_and_Other_Pacific_Islander_alone_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Native_Hawaiian_and_Other_Pacific_Islander_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Native_Hawaiian_and_Other_Pacific_Islander_alone_Percentage Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Some_other_race_alone_Value Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Some_other_race_alone_Margin_of_error Number(28,10),
Race_and_ethnicity_Hispanic_or_Latino_Some_other_race_alone_Percentage Number(28,10)
);
