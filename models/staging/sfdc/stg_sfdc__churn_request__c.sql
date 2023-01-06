SELECT *
FROM {{ source('SFDC', 'CHURN_REQUEST__C') }}