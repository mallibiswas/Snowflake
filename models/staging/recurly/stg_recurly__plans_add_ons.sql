SELECT * FROM {{ source('RECURLY', 'PLANS_ADD_ONS') }}