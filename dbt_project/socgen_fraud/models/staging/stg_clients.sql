with source as (
    select * from {{ source('socgen_raw', 'clients') }}
),

cleaned as (
    select
        id                                              as client_id,
        client_uid,
        segment::text                                   as segment,
        statut::text                                    as statut,
        agence_id,
        coalesce(revenu_mensuel_net, 0)                 as revenu_mensuel_net,
        coalesce(est_proprietaire, false)               as est_proprietaire,
        coalesce(pays_residence, 'FR')                  as pays_residence,
        ville_principale,
        date_entree_relation,
        extract('month' from age(date_entree_relation))::int +
        extract('year'  from age(date_entree_relation))::int * 12
                                                        as anciennete_mois,
        extract('year' from age(date_naissance))::int   as age,
        created_at
    from source
)

select * from cleaned