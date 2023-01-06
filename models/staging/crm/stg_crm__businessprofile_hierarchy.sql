SELECT Q1.L1_ID,
       Q1.L1_NAME,
       Q1.L1_ID                   AS PARENT_ID,
       -- parent name is the cleaned up parent name, then delete all special characters (preserving a few like "'","&","." etc)
       REGEXP_REPLACE(regexp_replace(L1_NAME, '(parent|group|Corporate|llc)', '', 1, 0, 'i'), '[^a-zA-Z0-9\'.& ]+', ' ',
                      1, 0, 'i')  AS PARENT_NAME,
       Q1.L1_ADDRESS,
       Q2.L2_ID,
       Q2.L2_NAME,
       Q3.L3_ID,
       Q3.L3_NAME,
       Q4.L4_ID,
       Q4.L4_NAME,
       Q5.L5_ID,
       Q5.L5_NAME,
       Q6.L6_ID,
       Q6.L6_NAME,
       CASE
           WHEN Q2.L2_ID IS NULL THEN Q1.L1_ID
           WHEN Q3.L3_ID IS NULL THEN Q2.L2_ID
           WHEN Q4.L4_ID IS NULL THEN Q3.L3_ID
           WHEN Q5.L5_ID IS NULL THEN Q4.L4_ID
           WHEN Q6.L6_ID IS NULL THEN Q5.L5_ID
           ELSE Q6.L6_ID END      AS BUSINESS_ID,
       CASE
           WHEN Q2.L2_ID IS NULL THEN Q1.L1_NAME
           WHEN Q3.L3_ID IS NULL THEN Q2.L2_NAME
           WHEN Q4.L4_ID IS NULL THEN Q3.L3_NAME
           WHEN Q5.L5_ID IS NULL THEN Q4.L4_NAME
           WHEN Q6.L6_ID IS NULL THEN Q5.L5_NAME
           ELSE Q6.L6_NAME END    AS BUSINESS_NAME,
       CASE
           WHEN Q2.L2_ID IS NULL THEN Q1.L1_ADDRESS
           WHEN Q3.L3_ID IS NULL THEN Q2.L2_ADDRESS
           WHEN Q4.L4_ID IS NULL THEN Q3.L3_ADDRESS
           WHEN Q5.L5_ID IS NULL THEN Q4.L4_ADDRESS
           WHEN Q6.L6_ID IS NULL THEN Q5.L5_ADDRESS
           ELSE Q6.L6_ADDRESS END AS ADDRESS,
        case when Q2.L2_active_fl is null then Q1.L1_active_fl
             when Q3.L3_active_fl is null then Q2.L2_active_fl
             when Q4.L4_active_fl is null then Q3.L3_active_fl
             when Q5.L5_active_fl is null then Q4.L4_active_fl
             when Q6.L6_active_fl is null then Q5.L5_active_fl
             else Q6.L6_active_fl end as active_location,
        case when Q2.L2_created_date is null then Q1.L1_created_date
             when Q3.L3_created_date is null then Q2.L2_created_date
             when Q4.L4_created_date is null then Q3.L3_created_date
             when Q5.L5_created_date is null then Q4.L4_created_date
             when Q6.L6_created_date is null then Q5.L5_created_date
             else Q6.L6_created_date end as created_date,
        case when Q2.L2_updated_date is null then Q1.L1_updated_date
             when Q3.L3_updated_date is null then Q2.L2_updated_date
             when Q4.L4_updated_date is null then Q3.L3_updated_date
             when Q5.L5_updated_date is null then Q4.L4_updated_date
             when Q6.L6_updated_date is null then Q5.L5_updated_date
             else Q6.L6_updated_date end as updated_date,
       NOT regexp_like(L1_NAME, '(.*)(fake|bogus|test|demo|playground|network|ZR Concepts|AdamBomb)(.*)',
                       'i')       AS VALID_REC
-- valid rec as determined by keyword in l1_name (parent name)
FROM (SELECT BUSINESS_ID AS L1_ID, NAME AS L1_NAME, ADDRESS AS L1_ADDRESS,
                 created_date as L1_created_date,
                 updated as L1_updated_date,
                 NOT(inactive) as L1_active_fl
      FROM {{ ref('stg_crm__portal_businessprofile') }}
      WHERE PARENT_ID IS NULL) Q1
         LEFT OUTER JOIN
     (SELECT BUSINESS_ID AS L2_ID, PARENT_ID AS L2_PARENT, NAME AS L2_NAME, ADDRESS AS L2_ADDRESS,
                 created_date as L2_created_date,
                 updated as L2_updated_date,
                 NOT(inactive) as L2_active_fl
      FROM {{ ref('stg_crm__portal_businessprofile') }}
      WHERE PARENT_ID IN (SELECT BUSINESS_ID FROM {{ ref('stg_crm__portal_businessprofile') }} WHERE PARENT_ID IS NULL)
     ) Q2 ON L1_ID = L2_PARENT -- Grand Parent
         LEFT OUTER JOIN
     (
         SELECT BUSINESS_ID AS L3_ID, PARENT_ID AS L3_PARENT, NAME AS L3_NAME, ADDRESS AS L3_ADDRESS,
                created_date as L3_created_date,
                updated as L3_updated_date,
                NOT(inactive) as L3_active_fl
         FROM {{ ref('stg_crm__portal_businessprofile') }}
         WHERE PARENT_ID IN (SELECT BUSINESS_ID
                             FROM {{ ref('stg_crm__portal_businessprofile') }}
                             WHERE PARENT_ID IN
                                   (SELECT BUSINESS_ID FROM {{ ref('stg_crm__portal_businessprofile') }} WHERE PARENT_ID IS NULL))
     ) Q3 ON L2_ID = L3_PARENT -- Parent
         LEFT OUTER JOIN
     (
         SELECT BUSINESS_ID AS L4_ID, PARENT_ID AS L4_PARENT, NAME AS L4_NAME, ADDRESS AS L4_ADDRESS,
                created_date as L4_created_date,
                updated as L4_updated_date,
                NOT(inactive) as L4_active_fl
         FROM {{ ref('stg_crm__portal_businessprofile') }}
         WHERE PARENT_ID IN (
             SELECT BUSINESS_ID
             FROM {{ ref('stg_crm__portal_businessprofile') }}
             WHERE PARENT_ID IN (SELECT BUSINESS_ID
                                 FROM {{ ref('stg_crm__portal_businessprofile') }}
                                 WHERE PARENT_ID IN
                                       (SELECT BUSINESS_ID
                                        FROM {{ ref('stg_crm__portal_businessprofile') }}
                                        WHERE PARENT_ID IS NULL))
         )
     ) Q4 ON L3_ID = L4_PARENT -- Child
         LEFT OUTER JOIN
     (
         SELECT BUSINESS_ID AS L5_ID, PARENT_ID AS L5_PARENT, NAME AS L5_NAME, ADDRESS AS L5_ADDRESS,
                created_date as L5_created_date,
                updated as L5_updated_date,
                NOT(inactive) as L5_active_fl
         FROM {{ ref('stg_crm__portal_businessprofile') }}
         WHERE PARENT_ID IN (
             SELECT BUSINESS_ID
             FROM {{ ref('stg_crm__portal_businessprofile') }}
             WHERE PARENT_ID IN (
                 SELECT BUSINESS_ID
                 FROM {{ ref('stg_crm__portal_businessprofile') }}
                 WHERE PARENT_ID IN (SELECT BUSINESS_ID
                                     FROM {{ ref('stg_crm__portal_businessprofile') }}
                                     WHERE PARENT_ID IN
                                           (SELECT BUSINESS_ID
                                            FROM {{ ref('stg_crm__portal_businessprofile') }}
                                            WHERE PARENT_ID IS NULL))
             )
         )
     ) Q5 ON L4_ID = L5_PARENT -- Grand Child
         LEFT OUTER JOIN
     (
         SELECT BUSINESS_ID AS L6_ID, PARENT_ID AS L6_PARENT, NAME AS L6_NAME, ADDRESS AS L6_ADDRESS,
                created_date as L6_created_date,
                updated as L6_updated_date,
                NOT(inactive) as L6_active_fl
         FROM {{ ref('stg_crm__portal_businessprofile') }}
         WHERE PARENT_ID IN (
             SELECT BUSINESS_ID
             FROM {{ ref('stg_crm__portal_businessprofile') }}
             WHERE PARENT_ID IN (
                 SELECT BUSINESS_ID
                 FROM {{ ref('stg_crm__portal_businessprofile') }}
                 WHERE PARENT_ID IN (
                     SELECT BUSINESS_ID
                     FROM {{ ref('stg_crm__portal_businessprofile') }}
                     WHERE PARENT_ID IN (SELECT BUSINESS_ID
                                         FROM {{ ref('stg_crm__portal_businessprofile') }}
                                         WHERE PARENT_ID IN
                                               (SELECT BUSINESS_ID
                                                FROM {{ ref('stg_crm__portal_businessprofile') }}
                                                WHERE PARENT_ID IS NULL))
                 )
             )
         )
     ) Q6 ON L5_ID = L6_PARENT -- Great Grand Child