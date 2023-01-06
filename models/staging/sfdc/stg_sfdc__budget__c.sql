SELECT *
FROM {{ source('SFDC', 'BUDGET__C') }}