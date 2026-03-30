with transactions as (
    select * from {{ ref('stg_transactions') }}
),

clients as (
    select * from {{ ref('stg_clients') }}
),

velocite as (
    select
        t.transaction_id,
        t.client_id,
        t.date_transaction,
        t.montant,

        count(*) over (
            partition by t.client_id
            order by t.date_transaction
            range between interval '1 hour' preceding and current row
        ) - 1                                       as nb_tx_1h,

        count(*) over (
            partition by t.client_id
            order by t.date_transaction
            range between interval '24 hours' preceding and current row
        ) - 1                                       as nb_tx_24h,

        sum(t.montant) over (
            partition by t.client_id
            order by t.date_transaction
            range between interval '1 hour' preceding and current row
        ) - t.montant                               as montant_cumul_1h,

        sum(t.montant) over (
            partition by t.client_id
            order by t.date_transaction
            range between interval '24 hours' preceding and current row
        ) - t.montant                               as montant_cumul_24h,

        avg(t.montant) over (
            partition by t.client_id
            order by t.date_transaction
            rows between unbounded preceding and 1 preceding
        )                                           as montant_moyen_historique,

        stddev(t.montant) over (
            partition by t.client_id
            order by t.date_transaction
            rows between unbounded preceding and 1 preceding
        )                                           as montant_stddev_historique,

        count(*) over (
            partition by t.client_id
            order by t.date_transaction
            range between interval '24 hours' preceding and current row
        )                                           as nb_pays_24h,

        count(*) over (
            partition by t.client_id
            order by t.date_transaction
            range between interval '1 hour' preceding and current row
        )                                           as nb_terminaux_24h

    from transactions t
),

final as (
    select
        t.transaction_id,
        t.client_id,
        t.montant,
        t.canal,
        t.type_transaction,
        t.mcc_code,
        t.pays_transaction,
        t.is_online,
        t.is_contactless,
        t.is_nuit,
        t.is_weekend,
        t.heure,
        t.jour_semaine,
        t.mois,
        t.distance_domicile_km,
        t.authentification,
        t.solde_apres,
        t.date_transaction,

        coalesce(v.nb_tx_1h, 0)                     as nb_tx_1h,
        coalesce(v.nb_tx_24h, 0)                     as nb_tx_24h,
        coalesce(v.montant_cumul_1h, 0)              as montant_cumul_1h,
        coalesce(v.montant_cumul_24h, 0)             as montant_cumul_24h,
        coalesce(v.nb_pays_24h, 1)                   as nb_pays_24h,
        coalesce(v.nb_terminaux_24h, 1)              as nb_terminaux_24h,

        case
            when coalesce(v.montant_moyen_historique, 0) > 0
            then round(cast(t.montant / v.montant_moyen_historique as numeric), 4)
            else 1
        end                                          as ratio_montant_vs_moyenne,

        case
            when coalesce(v.montant_stddev_historique, 0) > 0
            then round(cast(
                (t.montant - v.montant_moyen_historique)
                / v.montant_stddev_historique as numeric), 4)
            else 0
        end                                          as zscore_montant,

        coalesce(c.revenu_mensuel_net, 0)            as revenu_mensuel_net,
        coalesce(c.anciennete_mois, 0)               as anciennete_mois,
        coalesce(c.age, 30)                          as age_client,
        c.segment,

        case
            when t.pays_transaction != coalesce(c.pays_residence, 'FR') then true
            else false
        end                                          as pays_inhabituel,

        case
            when coalesce(c.revenu_mensuel_net, 0) > 0
            then round(cast(t.montant / c.revenu_mensuel_net as numeric), 4)
            else 0
        end                                          as ratio_montant_revenu

    from transactions t
    left join velocite v  on v.transaction_id = t.transaction_id
    left join clients  c  on c.client_id      = t.client_id
)

select * from final