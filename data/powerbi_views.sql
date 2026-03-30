
-- Vues PostgreSQL optimisées pour Power BI
-- SocGen Fraud Detection Dashboard


-- Vue 1 : KPIs globaux
CREATE OR REPLACE VIEW v_kpis_globaux AS
SELECT
    COUNT(*)                                        AS nb_transactions_total,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct,
    SUM(CASE WHEN fl.is_fraud THEN fl.fraud_amount ELSE 0 END)
                                                    AS montant_fraude_total,
    ROUND(AVG(t.montant), 2)                        AS montant_moyen_transaction,
    COUNT(DISTINCT t.client_id)                     AS nb_clients_actifs
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id;

-- Vue 2 : Fraudes par heure
CREATE OR REPLACE VIEW v_fraudes_par_heure AS
SELECT
    EXTRACT(HOUR FROM t.date_transaction)::INT      AS heure,
    COUNT(*)                                        AS nb_transactions,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
GROUP BY EXTRACT(HOUR FROM t.date_transaction)
ORDER BY heure;

-- Vue 3 : Fraudes par type
CREATE OR REPLACE VIEW v_fraudes_par_type AS
SELECT
    COALESCE(fl.fraud_type::text, 'legitime')       AS type_fraude,
    COUNT(*)                                        AS nb_cas,
    SUM(COALESCE(fl.fraud_amount, 0))               AS montant_total,
    ROUND(AVG(COALESCE(fl.fraud_amount, 0)), 2)     AS montant_moyen
FROM fraud_labels fl
WHERE fl.is_fraud = TRUE
GROUP BY fl.fraud_type
ORDER BY nb_cas DESC;

-- Vue 4 : Fraudes par canal
CREATE OR REPLACE VIEW v_fraudes_par_canal AS
SELECT
    t.canal::text                                   AS canal,
    COUNT(*)                                        AS nb_transactions,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct,
    ROUND(AVG(t.montant), 2)                        AS montant_moyen
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
GROUP BY t.canal
ORDER BY nb_fraudes DESC;

-- Vue 5 : Fraudes par jour (30 derniers jours)
CREATE OR REPLACE VIEW v_fraudes_par_jour AS
SELECT
    DATE(t.date_transaction)                        AS jour,
    COUNT(*)                                        AS nb_transactions,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    SUM(CASE WHEN fl.is_fraud THEN fl.fraud_amount ELSE 0 END)
                                                    AS montant_fraude
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
WHERE t.date_transaction >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(t.date_transaction)
ORDER BY jour;

-- Vue 6 : Fraudes par pays
CREATE OR REPLACE VIEW v_fraudes_par_pays AS
SELECT
    t.pays_transaction                              AS pays,
    COUNT(*)                                        AS nb_transactions,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
GROUP BY t.pays_transaction
ORDER BY nb_fraudes DESC;

-- Vue 7 : Fraudes par segment client
CREATE OR REPLACE VIEW v_fraudes_par_segment AS
SELECT
    c.segment::text                                 AS segment,
    COUNT(DISTINCT t.client_id)                     AS nb_clients,
    COUNT(*)                                        AS nb_transactions,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
LEFT JOIN clients c ON c.id = t.client_id
GROUP BY c.segment
ORDER BY nb_fraudes DESC;

-- Vue 8 : Top MCC frauduleux (type d’activité d’un commerçant)
CREATE OR REPLACE VIEW v_top_mcc_fraude AS
SELECT
    t.mcc_code,
    t.nom_commercant,
    COUNT(*)                                        AS nb_transactions,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)   AS nb_fraudes,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
GROUP BY t.mcc_code, t.nom_commercant
ORDER BY taux_fraude_pct DESC
LIMIT 20;

-- Vue 9 : Profil horaire fraude vs légitime
CREATE OR REPLACE VIEW v_profil_horaire AS
SELECT
    EXTRACT(HOUR FROM t.date_transaction)::INT      AS heure,
    COUNT(*) FILTER (WHERE fl.is_fraud = FALSE)     AS nb_legitimes,
    COUNT(*) FILTER (WHERE fl.is_fraud = TRUE)      AS nb_fraudes
FROM transactions t
LEFT JOIN fraud_labels fl ON fl.transaction_id = t.id
GROUP BY EXTRACT(HOUR FROM t.date_transaction)
ORDER BY heure;

-- Vue 10 : Performance modèle (depuis mart dbt)
CREATE OR REPLACE VIEW v_performance_modele AS
SELECT
    COUNT(*)                                        AS nb_total,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)      AS nb_fraudes,
    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                    AS taux_fraude_pct,
    ROUND(AVG(ratio_montant_vs_moyenne), 4)         AS ratio_montant_moyen,
    ROUND(AVG(zscore_montant), 4)                   AS zscore_moyen,
    ROUND(AVG(nb_tx_1h), 2)                         AS nb_tx_1h_moyen
FROM dbt_socgen_mart.mart_fraud_detection;