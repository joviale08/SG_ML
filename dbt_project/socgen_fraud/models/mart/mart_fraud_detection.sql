with features as (
    select * from {{ ref('int_fraud_features') }}
),

labels as (
    select * from {{ ref('stg_fraud_labels') }}
),

final as (
    select
        -- Identifiants
        f.transaction_id,
        f.client_id,
        f.date_transaction,

        -- Variable cible
        coalesce(l.is_fraud, false)             as is_fraud,
        l.fraud_type,

        -- Features transaction
        f.montant,
        f.canal,
        f.type_transaction,
        f.mcc_code,
        f.pays_transaction,
        f.is_online::int                        as is_online,
        f.is_contactless::int                   as is_contactless,
        f.is_nuit::int                          as is_nuit,
        f.is_weekend::int                       as is_weekend,
        f.heure,
        f.jour_semaine,
        f.mois,
        f.distance_domicile_km,
        f.authentification,
        f.solde_apres,

        -- Features vélocité
        f.nb_tx_1h,
        f.nb_tx_24h,
        f.montant_cumul_1h,
        f.montant_cumul_24h,
        f.nb_pays_24h,
        f.nb_terminaux_24h,

        -- Features statistiques
        f.ratio_montant_vs_moyenne,
        f.zscore_montant,
        f.ratio_montant_revenu,

        -- Features client
        f.revenu_mensuel_net,
        f.anciennete_mois,
        f.age_client,
        f.segment,
        f.pays_inhabituel::int                  as pays_inhabituel,

        -- Métadonnées
        current_timestamp                       as computed_at

    from features f
    left join labels l on l.transaction_id = f.transaction_id
)

select * from final