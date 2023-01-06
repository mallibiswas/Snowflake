-------------------------------------------------------------------
---------- Salesforce Opportunities
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());


create or replace table &{stageschemaname}.d_sfdc_opportunities
as
  SELECT
    SUBSTR(SFDC.OPPORTUNITY.ID,1,15) as SALESFORCE_OPPORTUNITY_ID,
    SFDC.OPPORTUNITY.TYPE as OPPORTUNITY_TYPE,
    STAGENAME,
    CASE
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='aaron@greenfieldlabs.ca' THEN 'Aaron Scriver'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='adam@zenreach.com' THEN 'Adam Cohen'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='agillaspie@zenreach.com' THEN 'Alex Gillespie'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='aborella@zenreach.com' THEN 'Andrew Borella'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='bchiang@zenreach.com' THEN 'Ben Chiang'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='bperman@zenreach.com' THEN 'Blake Perman'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='corey@zenreach.com' THEN 'Corey Warfield'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='csnyder@zenreach.com' THEN 'Cory Snyder'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='daniel@zenreach.com' THEN 'Daniel Strazulo'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='dmacdonald@zenreach.com' THEN 'Devon Macdonald'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='jboughton@zenreach.com' THEN 'Jeff Boughton'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='jkelly@zenreach.com' THEN 'John Kelly'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='kdelena@zenreach.com' THEN 'Kelly D\'Elena'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='pdonahoe@zenreach.com' THEN 'Patrick Donahoe'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='rcornell@zenreach.com' THEN 'Robert Cornell'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='rmitchell@zenreach.com' THEN 'Robert Mitchell-Crossley'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='alexb@zenreach.com' THEN 'Alex Heal'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='brube@zenreach.com' THEN 'Brent Rube'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='devon@zenreach.com' THEN 'Devon Kerns'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='eking@zenreach.com' THEN 'Emily King'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='ian@zenreach.com' THEN 'Ian Bishop'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='luke@zenreach.com' THEN 'Luke Januschka'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='paul@zenreach.com' THEN 'Paul Slavik'
        WHEN SFDC.OPPORTUNITY.OWNER_MANAGER__C ='spradlin@zenreach.com' THEN 'Micheal Spradlin'
        ELSE SFDC.OPPORTUNITY.OWNER_MANAGER__C
    END AS OWNER_MANAGER,
    SFDC.OPPORTUNITY.OWNERID,
    SFDC.USER.FIRSTNAME,
    SFDC.USER.LASTNAME,
    CONCAT(SFDC.USER.FIRSTNAME,' ', SFDC.USER.LASTNAME) as OPPORTUNITY_OWNER,
    SFDC.USER.ISACTIVE as OPPORTUNITY_OWNER_ISACTIVE,
    SFDC.OPPORTUNITY.ACCOUNTID,
    SFDC.OPPORTUNITY.NAME as OPPORTUNITY_NAME,
    SFDC.ACCOUNT.NAME as ACCOUNT_NAME,
    TO_DATE(SFDC.OPPORTUNITY.CLOSEDATE) CLOSEDATE,
    DATE_TRUNC(month,SFDC.OPPORTUNITY.CLOSEDATE)::date CLOSEMONTH,
    SFDC.OPPORTUNITY.AMOUNT as OPPORTUNITY_AMOUNT,
    current_date() as asof_date
  FROM
            SFDC.OPPORTUNITY
  LEFT JOIN SFDC.USER ON SFDC.USER.Id = SFDC.OPPORTUNITY.OWNERID
  LEFT JOIN SFDC.ACCOUNT ON SFDC.ACCOUNT.Id = SFDC.OPPORTUNITY.ACCOUNTID
  WHERE   ((OPPORTUNITY.ISCLOSED = TRUE AND OPPORTUNITY.ISWON = TRUE) OR OPPORTUNITY.stagename = 'Stage 4: Decision')
;


alter table &{stageschemaname}.d_sfdc_opportunities  swap with &{schemaname}.d_sfdc_opportunities;
