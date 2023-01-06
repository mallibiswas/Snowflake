{{ config(materialized='table') }}

with customer_profile_crm_ as (
  select 
    t.business_id
    , t.pos_customer_id
    , t.pos_name
    , t.pos_email
    , c.contact
    , c.fullname
    , c.in_business_network
    , crm_score(pos_name, pos_email, fullname, contact) name_score
  From {{ref('mart_merged_presence_pos__pos_customers')}} t
       , {{ref('mart_merged_presence_pos__local_crm_contacts')}} c
  where t.business_id = c.business_id
      -- to limit the scope of the join require that the point of sale last name appears 
      -- in its entirety in the zenreach email or full name
      -- or there is an exact email match
      -- THIS version of the join allows more matches, but requires a lot more expensive "contains"
      -- comparisons, which really makes this query extremely slow to run.
      -- run time __x joining on initials or exact email match
     and 
        case 
            -- if the POS has an email, look for an exact match to this email
            -- in practice this isn't the most interesting case, as we already have an email address
            -- that has a moderate probability of being valid
            -- we may miss some opportunities for "alternative" email address, but we'll let that go for now.
            when pos_email is not null then pos_email = contact
            -- if there is no email on the transaction, then attempt basic string matching
            else (contains(contact, pos_last_name) or contains(last_name, pos_last_name))
        end
    
      -- Tried joining on initials hoping that since snowflake auto-clusters that an exact match join on itinials 
      -- would speed up the query, rather than performing 'contains' matches on last names (version above)
      -- albeit at the cost of dropping potential email <> pos_name matches (e.g. carrie.isaacson@gmail.com <> carrie isaacson)
      -- but I believe this trade off could be mitigated by inferring names from emails usign python segment/nameparser on mart_merged_presence_pos__contact_details
      -- *however* the improvement does not appear to bear out as expected, run times weren't so far off either way
      -- at the expense of fewer matches:
        -- and t.pos_initials is not null
        -- and c.initials is not null
        -- and (t.pos_initials = c.initials or pos_email = contact)
)
, customer_profiles__ as (
    select distinct c.business_id, c.contact, c.fullname, c.pos_customer_id, c.pos_email, c.pos_name, iff(c.in_business_network, 'merchant', 'localized') as source
    from customer_profile_crm_ c
    left join {{ref('mart_merged_presence_pos__temporospatial_contact_profiles')}} tempo
        on c.contact = tempo.zenreach_contact and c.pos_customer_id = tempo.pos_customer_id
    where name_score <= 0
          and tempo.pos_customer_id is null -- if there are temporospatial matches for this customer ID, don't create alternate Merchant CRM / Local CRM matches
)
, customer_profiles_ as (
  select 
    *
    , count(distinct contact) over (partition by business_id, pos_customer_id, source) as alternate_source_matches
    , count(distinct contact) over (partition by business_id, pos_customer_id) as alternate_matches
    , max(source) over (partition by pos_customer_id) as best_source
  from customer_profiles__
)
select 
     contact as zenreach_contact
    , fullname as zenreach_fullname
    , pos_customer_id
    , pos_email
    , pos_name
    , source
    , business_id
    , alternate_source_matches
    , alternate_matches
    , best_source
    , current_timestamp() as created_at
from customer_profiles_
where 
    -- A name match is accepted only if it joins on 3 or fewer different emails w/in network or localized (in merchant area)
    -- or 4 distinct emails overall.
    -- So if POS_ID 123445678 Carrie Isaacson matched on two merchant crm entries (carrie.isaacson@gmail.com, carrie.e.isaacson@yahoo.com)
    -- and matched on two localized crm entries (carrie.isaacson77@gmail.com, carrie isaacso + cisaacson77@yahoo.com)
    -- then we would not attempt to use any of these emails for presence merging / attribution.
    ((source = 'merchant' and alternate_source_matches <= 3) or (source = 'localized' and alternate_source_matches <= 3))
    and (alternate_matches < 4)
    and (source = best_source) -- if there's are merchant crm matches, choose that over local matches
order by pos_name
