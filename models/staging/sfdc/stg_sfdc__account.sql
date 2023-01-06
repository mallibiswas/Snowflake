SELECT *
FROM {{ source('SFDC', 'ACCOUNT') }}