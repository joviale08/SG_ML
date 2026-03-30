with source as (
    select * from {{ source('socgen_raw', 'fraud_labels') }}
),

cleaned as (
    select
        id                          as label_id,
        transaction_id,
        client_id,
        is_fraud,
        fraud_type::text            as fraud_type,
        coalesce(fraud_amount, 0)   as fraud_amount,
        fraud_reported_at,
        fraud_confirmed_at,
        coalesce(chargeback_raised, false) as chargeback_raised,
        labeling_version,
        created_at
    from source
)

select * from cleaned