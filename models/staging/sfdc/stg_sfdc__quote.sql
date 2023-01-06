SELECT *
FROM {{ source('SFDC', 'QUOTE') }}