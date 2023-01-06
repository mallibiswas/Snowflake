{{ config(materialized='table') }}

with analytics_customer_ as (
  select
    iff(contains(ac.email, '@'),clean_contact(ac.email), ac.email) as contact
    , iff(ac.age='' or ac.age='None',NULL,ac.age) as age
    , iff(ac.gender='' or ac.gender='None',NULL,ac.gender) as gender
    , iff(ac.income='' or ac.income='None',NULL,ac.income) as income
    -- clean full name omit punctuation, dates, etc
    , clean_name(ac.fullname) as fullname
    , created
    -- how many distinct analytics customer entries have been created under this email
    -- very large numbers are not reliable emails (e.g. a@a.com = 5053 entries, h@gmail.com = 1313 entries, etc.)
    -- here we'll use a simple heuristic limiting to 500 distinct entries
    , count(*) over (partition by contact) n_entries

  from {{ seed_or_ref(ref('stg_crm__analytics_customer'), 'SEED_ANALYTICS_CUSTOMER') }} ac

  where ac.email not like '%@qos.zenreach.com'
    and ac.email not like '%@chowlyinc.com'
    and (ac.email like '%@%' or len(ac.email) <> 24) -- intended to delete entries where contact_info = business_id but keep phone numbers (maybe not the right call) 
)
, email_scores_ as (
  select 
      iff(contains(up.email, '@'),clean_contact(up.email), up.email) as contact
      , email_is_valid
      , email_reason
      , email_score
      , date_added

  from {{ seed_or_ref(ref('stg_crm__portal_userprofile'), 'SEED_PORTAL_USERPROFILE') }} up

  where up.email not like '%@qos.zenreach.com'
        and up.email not like '%@chowlyinc.com'
)
-- Use window functions to select the first non-null age, gender, income & name rank ordered by valid emails & email score
-- merging across cleaned versions of the same email (e.g. c.isaacson@gmail.con == cisaacson@gmail.com)
, best_available_metrics_ as (
  select
    ac.contact
    , first_value(ac.age) IGNORE NULLS over (partition by ac.contact order by es.email_is_valid desc, es.email_score desc, ac.created desc) as age
    , first_value(ac.gender) IGNORE NULLS over (partition by ac.contact order by es.email_is_valid desc, es.email_score desc, ac.created desc) as gender
    , first_value(ac.income) IGNORE NULLS over (partition by ac.contact order by es.email_is_valid desc, es.email_score desc, ac.created desc) as income
    , first_value(ac.fullname) IGNORE NULLS over (partition by ac.contact order by es.email_is_valid desc, es.email_score desc, ac.created desc) as fullname
    , es.email_is_valid
    , es.email_reason
    , es.email_score
    , row_number() over (partition by ac.contact order by es.email_is_valid desc, es.email_score desc, ac.created desc) as rank
    , ac.n_entries
    , es.date_added
  from analytics_customer_ ac
       , email_scores_ es
  where ac.contact = es.contact
        -- thorw out emails that appear in more than 500 CRMs
        and ac.n_entries < 500
)
select contact, age, gender, income, fullname, email_is_valid, email_reason, email_score, n_entries, date_added, current_timestamp() as last_run
from best_available_metrics_
where rank = 1 and contact is not NULL
order by n_entries desc
