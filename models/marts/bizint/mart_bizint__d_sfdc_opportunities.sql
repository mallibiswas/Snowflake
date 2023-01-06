SELECT SUBSTR(OPPORTUNITY.ID, 1, 15)              AS  SALESFORCE_OPPORTUNITY_ID,
       OPPORTUNITY.TYPE                           AS  OPPORTUNITY_TYPE,
       OPPORTUNITY.STAGENAME,
       -- TODO: Move below to a lookup table.
       CASE
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'aaron@greenfieldlabs.ca' THEN 'Aaron Scriver'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'adam@zenreach.com' THEN 'Adam Cohen'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'agillaspie@zenreach.com' THEN 'Alex Gillespie'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'aborella@zenreach.com' THEN 'Andrew Borella'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'bchiang@zenreach.com' THEN 'Ben Chiang'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'bperman@zenreach.com' THEN 'Blake Perman'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'corey@zenreach.com' THEN 'Corey Warfield'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'csnyder@zenreach.com' THEN 'Cory Snyder'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'daniel@zenreach.com' THEN 'Daniel Strazulo'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'dmacdonald@zenreach.com' THEN 'Devon Macdonald'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'jboughton@zenreach.com' THEN 'Jeff Boughton'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'jkelly@zenreach.com' THEN 'John Kelly'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'kdelena@zenreach.com' THEN 'Kelly D\'Elena'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'pdonahoe@zenreach.com' THEN 'Patrick Donahoe'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'rcornell@zenreach.com' THEN 'Robert Cornell'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'rmitchell@zenreach.com' THEN 'Robert Mitchell-Crossley'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'alexb@zenreach.com' THEN 'Alex Heal'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'brube@zenreach.com' THEN 'Brent Rube'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'devon@zenreach.com' THEN 'Devon Kerns'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'eking@zenreach.com' THEN 'Emily King'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'ian@zenreach.com' THEN 'Ian Bishop'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'luke@zenreach.com' THEN 'Luke Januschka'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'paul@zenreach.com' THEN 'Paul Slavik'
           WHEN OPPORTUNITY.OWNER_MANAGER__C = 'spradlin@zenreach.com' THEN 'Micheal Spradlin'
           ELSE OPPORTUNITY.OWNER_MANAGER__C
           END                                    AS  OWNER_MANAGER,
       OPPORTUNITY.OWNERID,
       USER.FIRSTNAME,
       USER.LASTNAME,
       CONCAT(USER.FIRSTNAME, ' ', USER.LASTNAME) AS  OPPORTUNITY_OWNER,
       USER.ISACTIVE                              AS  OPPORTUNITY_OWNER_ISACTIVE,
       OPPORTUNITY.ACCOUNTID,
       OPPORTUNITY.NAME                           AS  OPPORTUNITY_NAME,
       ACCOUNT.NAME                               AS  ACCOUNT_NAME,
       TO_DATE(OPPORTUNITY.CLOSEDATE)                 CLOSEDATE,
       DATE_TRUNC(MONTH, OPPORTUNITY.CLOSEDATE)::DATE CLOSEMONTH,
       OPPORTUNITY.AMOUNT                         AS  OPPORTUNITY_AMOUNT,
       current_date()                             AS  ASOF_DATE
FROM {{ ref('stg_sfdc__opportunity') }} AS OPPORTUNITY
         LEFT JOIN {{ ref('stg_sfdc__user') }} USER ON USER.ID = OPPORTUNITY.OWNERID
         LEFT JOIN {{ ref('stg_sfdc__account') }} ACCOUNT ON ACCOUNT.ID = OPPORTUNITY.ACCOUNTID
WHERE ((OPPORTUNITY.ISCLOSED = TRUE AND OPPORTUNITY.ISWON = TRUE) OR OPPORTUNITY.STAGENAME = 'Stage 4: Decision')