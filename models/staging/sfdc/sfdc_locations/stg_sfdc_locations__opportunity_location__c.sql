SELECT *
FROM {{ source('SFDC_LOCATIONS', 'OPPORTUNITY_LOCATION__C') }}