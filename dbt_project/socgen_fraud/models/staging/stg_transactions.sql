with source as (
    select * from {{ source('socgen_raw', 'transactions') }}
),

cleaned as (
    select
        id                                    as transaction_id,
        transaction_uid,
        compte_id,
        client_id,
        montant,
        devise,
        type_transaction::text                as type_transaction,
        canal::text                           as canal,
        terminal_id,
        mcc_code,
        nom_commercant,
        coalesce(pays_transaction, 'FR')      as pays_transaction,
        latitude,
        longitude,
        coalesce(distance_domicile_km, 0)     as distance_domicile_km,
        coalesce(is_online, false)            as is_online,
        coalesce(is_contactless, false)       as is_contactless,
        ip_country,
        coalesce(authentification, 'pin')     as authentification,
        coalesce(solde_apres, 0)              as solde_apres,
        date_transaction,
        extract(hour from date_transaction)::int   as heure,
        extract(dow  from date_transaction)::int   as jour_semaine,
        extract(month from date_transaction)::int  as mois,
        case
            when extract(hour from date_transaction) between 0 and 5 then true
            else false
        end                                   as is_nuit,
        case
            when extract(dow from date_transaction) in (0, 6) then true
            else false
        end                                   as is_weekend,
        created_at
    from source
)

select * from cleaned