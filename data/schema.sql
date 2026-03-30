CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE statut_client     AS ENUM ('actif', 'inactif', 'suspendu', 'clos');
CREATE TYPE segment_client    AS ENUM ('particulier', 'premium', 'private', 'professionnel');
CREATE TYPE type_transaction  AS ENUM (
    'paiement_carte', 'retrait_dab', 'virement_entrant',
    'virement_sortant', 'prelevement', 'frais_bancaires'
);
CREATE TYPE canal_transaction AS ENUM ('carte_physique', 'online', 'dab', 'sans_contact', 'virement');
CREATE TYPE type_fraude       AS ENUM (
    'card_not_present', 'stolen_card', 'account_takeover',
    'social_engineering', 'skimming', 'friendly_fraud'
);

CREATE TABLE agences (
    id          SERIAL PRIMARY KEY,
    code_agence VARCHAR(10)  NOT NULL UNIQUE,
    nom         VARCHAR(100) NOT NULL,
    region      VARCHAR(50)  NOT NULL,
    ville       VARCHAR(80)  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE clients (
    id                   SERIAL PRIMARY KEY,
    client_uid           UUID          NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    prenom_hash          VARCHAR(64)   NOT NULL,
    nom_hash             VARCHAR(64)   NOT NULL,
    date_naissance       DATE          NOT NULL,
    code_postal          CHAR(5)       NOT NULL,
    departement          CHAR(3)       NOT NULL,
    segment              segment_client NOT NULL DEFAULT 'particulier',
    statut               statut_client  NOT NULL DEFAULT 'actif',
    agence_id            INT           REFERENCES agences(id),
    revenu_mensuel_net   NUMERIC(10,2),
    est_proprietaire     BOOLEAN       DEFAULT FALSE,
    pays_residence       CHAR(2)       DEFAULT 'FR',
    ville_principale     VARCHAR(80),
    date_entree_relation DATE          NOT NULL,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_clients_agence  ON clients(agence_id);
CREATE INDEX idx_clients_segment ON clients(segment);

CREATE TABLE comptes (
    id                SERIAL PRIMARY KEY,
    compte_uid        UUID          NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    client_id         INT           NOT NULL REFERENCES clients(id),
    type_compte       VARCHAR(20)   NOT NULL DEFAULT 'courant',
    statut            VARCHAR(20)   NOT NULL DEFAULT 'actif',
    solde_actuel      NUMERIC(14,2) NOT NULL DEFAULT 0,
    plafond_decouvert NUMERIC(10,2) DEFAULT 0,
    date_ouverture    DATE          NOT NULL,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_comptes_client ON comptes(client_id);

CREATE TABLE transactions (
    id                   BIGSERIAL     PRIMARY KEY,
    transaction_uid      UUID          NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    compte_id            INT           NOT NULL REFERENCES comptes(id),
    client_id            INT           NOT NULL REFERENCES clients(id),
    montant              NUMERIC(12,2) NOT NULL,
    devise               CHAR(3)       NOT NULL DEFAULT 'EUR',
    type_transaction     type_transaction NOT NULL,
    canal                canal_transaction NOT NULL,
    terminal_id          VARCHAR(20),
    mcc_code             CHAR(4),
    nom_commercant       VARCHAR(120),
    pays_transaction     CHAR(2)       DEFAULT 'FR',
    latitude             NUMERIC(9,6),
    longitude            NUMERIC(9,6),
    distance_domicile_km NUMERIC(8,2),
    is_online            BOOLEAN       DEFAULT FALSE,
    is_contactless       BOOLEAN       DEFAULT FALSE,
    device_fingerprint   VARCHAR(64),
    ip_country           CHAR(2),
    authentification     VARCHAR(20)   DEFAULT 'pin',
    solde_apres          NUMERIC(14,2),
    date_transaction     TIMESTAMPTZ   NOT NULL,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tx_client_date ON transactions(client_id, date_transaction DESC);
CREATE INDEX idx_tx_compte_date ON transactions(compte_id, date_transaction DESC);
CREATE INDEX idx_tx_date        ON transactions(date_transaction DESC);
CREATE INDEX idx_tx_terminal    ON transactions(terminal_id);
CREATE INDEX idx_tx_pays        ON transactions(pays_transaction);
CREATE INDEX idx_tx_online      ON transactions(is_online, date_transaction DESC);

CREATE TABLE fraud_labels (
    id                 BIGSERIAL   PRIMARY KEY,
    transaction_id     BIGINT      NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    client_id          INT         NOT NULL REFERENCES clients(id),
    is_fraud           BOOLEAN     NOT NULL,
    fraud_type         type_fraude,
    fraud_amount       NUMERIC(10,2),
    fraud_reported_at  TIMESTAMPTZ,
    fraud_confirmed_at TIMESTAMPTZ,
    chargeback_raised  BOOLEAN     DEFAULT FALSE,
    labeling_version   VARCHAR(10) DEFAULT 'v1.0',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_fraud_tx_uniq ON fraud_labels(transaction_id, labeling_version);
CREATE INDEX        idx_fraud_client  ON fraud_labels(client_id, is_fraud);
CREATE INDEX        idx_fraud_date    ON fraud_labels(fraud_confirmed_at DESC) WHERE is_fraud = TRUE;

CREATE TABLE card_blocks (
    id             SERIAL      PRIMARY KEY,
    client_id      INT         NOT NULL REFERENCES clients(id),
    compte_id      INT         REFERENCES comptes(id),
    motif          VARCHAR(50) NOT NULL,
    date_blocage   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    date_deblocage TIMESTAMPTZ,
    initie_par     VARCHAR(20) DEFAULT 'systeme',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_blocks_client ON card_blocks(client_id, date_blocage DESC);