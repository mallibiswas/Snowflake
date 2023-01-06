-------------------------------------------------------------------
---------- Summary of Opprtunity Line Items from Salesforce
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table &{stageschemaname}.SFDC_OLI_summary_fact
as
  SELECT
    SUBSTR(SFDC.OPPORTUNITY.ID,1,15) as OPPORTUNITY_ID,
    CASE     WHEN SFDC.OPPORTUNITYLINEITEM.PRODUCTCODE = 'Ads' THEN 'Ads'
             WHEN SFDC.OPPORTUNITYLINEITEM.PRODUCTCODE IN ('Classic','Acquire','Retain','Build') THEN 'Core'
             ELSE SFDC.OPPORTUNITYLINEITEM.PRODUCTCODE
        END AS PRODUCT_CATEGORY,
    STAGENAME,
    SUM(SFDC.OPPORTUNITYLINEITEM.QUANTITY) OLI_QUANTITY,
    SUM(SFDC.OPPORTUNITYLINEITEM.UNITPRICE) OLI_UNITPRICE,
    SUM(SFDC.OPPORTUNITYLINEITEM.SUBTOTAL) OLI_SUBTOTAL,
    SUM(SFDC.OPPORTUNITYLINEITEM.TOTALPRICE) OLI_TOTALPRICE
  FROM
            SFDC.OPPORTUNITY
  LEFT JOIN SFDC.OPPORTUNITYLINEITEM
         ON SFDC.OPPORTUNITYLINEITEM.OPPORTUNITYID = SFDC.OPPORTUNITY.ID
 WHERE   ((OPPORTUNITY.ISCLOSED = TRUE AND OPPORTUNITY.ISWON = TRUE) OR OPPORTUNITY.stagename = 'Stage 4: Decision')
  GROUP BY OPPORTUNITY_ID, PRODUCT_CATEGORY, STAGENAME
;

alter table &{stageschemaname}.SFDC_OLI_summary_fact add column asof_date date;

update &{stageschemaname}.SFDC_OLI_summary_fact set asof_date=current_date();

alter table &{stageschemaname}.SFDC_OLI_summary_fact swap with &{schemaname}.SFDC_OLI_summary_fact;
