select * from zendev.audiences.user_profile
where array_size(ages)>1; -- find users/emails with multiple age entries in analytics_customer

select  business_id,
        email,
        ages[0]::string as age,
        cities[0]::string as city,
        genders[0]::string as gender,
        incomes[0]::string as income
from zenalytics.audiences.audience a LEFT JOIN zenalytics.audiences.user_profile p
where a.customer_sk = p.customer_sk
limit 100;

select  business_id,
        email,
        ages[0]::string as age,
        cities[0]::string as city,
        genders[0]::string as gender,
        incomes[0]::string as income
from zenalytics.audiences.audience a LEFT JOIN zenalytics.audiences.user_profile p
where a.customer_sk = p.customer_sk
limit 100;
