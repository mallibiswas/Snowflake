SELECT *
FROM {{ source('SFDC', 'ORDER') }}