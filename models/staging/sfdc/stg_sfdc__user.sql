SELECT *
FROM {{ source('SFDC', 'USER') }}