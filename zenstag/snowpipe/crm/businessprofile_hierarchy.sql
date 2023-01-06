CREATE OR REPLACE view ZENSTAG.CRM.BUSINESSPROFILE_HIERARCHY
comment='Business Profile Hierarchy View' 
AS 
SELECT  Q1.L1_id, 
        Q1.L1_name, 
        Q1.L1_id as parent_id,
        -- parent name is the cleaned up parent name, then delete all special characters (preserving a few like "'","&","." etc)
        REGEXP_REPLACE(regexp_replace(l1_name,'(parent|group|Corporate|llc)','',1,0,'i'),'[^a-zA-Z0-9\'.& ]+',' ',1,0,'i') as parent_name,        
        Q1.L1_address, 
        Q2.L2_id, 
        Q2.L2_name, 
        Q3.L3_id, 
        Q3.L3_name, 
        Q4.L4_id, 
        Q4.L4_name, 
        Q5.L5_id, 
        Q5.L5_name, 
        Q6.L6_id, 
        Q6.L6_name,
        case when Q2.L2_id is null then Q1.L1_id 
             when Q3.L3_id is null then Q2.L2_id 
             when Q4.L4_id is null then Q3.L3_id 
             when Q5.L5_id is null then Q4.L4_id 
             when Q6.L6_id is null then Q5.L5_id 
             else Q6.L6_id end as business_id,
        case when Q2.L2_id is null then Q1.L1_name 
             when Q3.L3_id is null then Q2.L2_name 
             when Q4.L4_id is null then Q3.L3_name 
             when Q5.L5_id is null then Q4.L4_name 
             when Q6.L6_id is null then Q5.L5_name 
             else Q6.L6_name end as business_name,
        case when Q2.L2_id is null then Q1.L1_address 
             when Q3.L3_id is null then Q2.L2_address 
             when Q4.L4_id is null then Q3.L3_address 
             when Q5.L5_id is null then Q4.L4_address 
             when Q6.L6_id is null then Q5.L5_address 
             else Q6.L6_address end as address,
NOT regexp_like(l1_name, '(.*)(fake|bogus|test|demo|playground|network|ZR Concepts|AdamBomb)(.*)','i') as valid_rec
-- valid rec as determined by keyword in l1_name (parent name)             
FROM
(select business_id as L1_id, name as L1_name, address as L1_address
from zenstag.crm.portal_businessprofile where parent_id is null) Q1
LEFT OUTER JOIN
(select business_id as L2_id, parent_id as L2_Parent, name as L2_name, address as L2_address
from zenstag.crm.portal_businessprofile
where parent_id in (select business_id from zenstag.crm.portal_businessprofile where parent_id is null)
) Q2 ON L1_id = L2_parent -- Grand Parent
LEFT OUTER JOIN
(
select business_id as L3_id, parent_id as L3_Parent, name as L3_name, address as L3_address 
from zenstag.crm.portal_businessprofile
where parent_id in (select business_id
                    from zenstag.crm.portal_businessprofile
                    where parent_id in
                    (select business_id from zenstag.crm.portal_businessprofile where parent_id is null))
) Q3 ON L2_id = L3_parent -- Parent
LEFT OUTER JOIN 
(
select business_id as L4_id, parent_id as L4_Parent, name as L4_name, address as L4_address 
from zenstag.crm.portal_businessprofile
where parent_id in (
select business_id from zenstag.crm.portal_businessprofile
where parent_id in (select business_id
                    from zenstag.crm.portal_businessprofile
                    where parent_id in
                          (select business_id from zenstag.crm.portal_businessprofile where parent_id is null))
    )
) Q4 on  L3_id = L4_Parent -- Child
LEFT OUTER JOIN
(
select business_id as L5_id, parent_id as L5_Parent, name as L5_name, address as L5_address 
from zenstag.crm.portal_businessprofile
where parent_id in (
select business_id from zenstag.crm.portal_businessprofile
where parent_id in (
select business_id from zenstag.crm.portal_businessprofile
where parent_id in (select business_id
                    from zenstag.crm.portal_businessprofile
                    where parent_id in
                          (select business_id from zenstag.crm.portal_businessprofile where parent_id is null))
)
)
) Q5 on  L4_id = L5_Parent -- Grand Child
LEFT OUTER JOIN
(
select business_id as L6_id, parent_id as L6_Parent, name as L6_name, address as L6_address 
from zenstag.crm.portal_businessprofile
where parent_id in (
select business_id from zenstag.crm.portal_businessprofile
where parent_id in (
select business_id from zenstag.crm.portal_businessprofile
where parent_id in (
select business_id from zenstag.crm.portal_businessprofile
where parent_id in (select business_id
                    from zenstag.crm.portal_businessprofile
                    where parent_id in
                          (select business_id from zenstag.crm.portal_businessprofile where parent_id is null))
    )
    )
    )
) Q6 on  L5_id = L6_Parent -- Great Grand Child;
;
