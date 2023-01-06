SELECT *
FROM {{ source('RECURLY', 'PLANS') }} A

