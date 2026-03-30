"""
Société Générale — Fraud Detection
Générateur de données synthétiques réalistes
Usage : python data/seed.py --clients 20000
"""

import argparse
import hashlib
import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from faker import Faker
from loguru import logger
from psycopg2.extras import execute_values
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

load_dotenv()

fake    = Faker("fr_FR")
console = Console()
np.random.seed(42)

DB_URL      = os.getenv("DATABASE_URL", "postgresql://socgen:socgen_secret@localhost:5432/socgen_fraud")
TAUX_FRAUDE = 0.02
NB_TX_MIN   = 150
NB_TX_MAX   = 200

SEGMENTS = {"particulier": 0.72, "premium": 0.18, "private": 0.04, "professionnel": 0.06}
REVENUS  = {"particulier": (1800, 3500), "premium": (3500, 8000),
            "private": (8000, 40000), "professionnel": (2500, 7000)}

AGENCES = [
    ("AG001", "SG Paris Opéra",         "Île-de-France",              "Paris"),
    ("AG002", "SG Paris La Défense",    "Île-de-France",              "Puteaux"),
    ("AG003", "SG Lyon Bellecour",      "Auvergne-Rhône-Alpes",       "Lyon"),
    ("AG004", "SG Marseille Canebière", "Provence-Alpes-Côte d'Azur", "Marseille"),
    ("AG005", "SG Bordeaux Tourny",     "Nouvelle-Aquitaine",         "Bordeaux"),
    ("AG006", "SG Toulouse Capitole",   "Occitanie",                  "Toulouse"),
    ("AG007", "SG Lille Grand Place",   "Hauts-de-France",            "Lille"),
    ("AG008", "SG Strasbourg Centre",   "Grand Est",                  "Strasbourg"),
    ("AG009", "SG Nantes Commerce",     "Pays de la Loire",           "Nantes"),
    ("AG010", "SG Nice Promenade",      "Provence-Alpes-Côte d'Azur", "Nice"),
]

MCC_CATEGORIES = [
    ("5411", "Supermarché",     0.22, (10,  150)),
    ("5812", "Restaurant",      0.15, (8,   80)),
    ("5541", "Station essence", 0.10, (30,  120)),
    ("5912", "Pharmacie",       0.06, (5,   60)),
    ("5311", "Grand magasin",   0.07, (20,  300)),
    ("4111", "Transport",       0.05, (5,   50)),
    ("5999", "Divers commerce", 0.08, (10,  100)),
    ("7011", "Hôtel",           0.03, (80,  500)),
    ("4722", "Agence voyages",  0.02, (200, 2000)),
    ("5945", "Electronique",    0.03, (50,  800)),
    ("5734", "Logiciels",       0.04, (10,  100)),
    ("7922", "Loisirs",         0.04, (15,  200)),
    ("5651", "Vêtements",       0.06, (20,  250)),
    ("6011", "Retrait DAB",     0.05, (20,  500)),
]
MCC_CODES    = [m[0] for m in MCC_CATEGORIES]
MCC_NOMS     = [m[1] for m in MCC_CATEGORIES]
MCC_WEIGHTS  = np.array([m[2] for m in MCC_CATEGORIES], dtype=float)
MCC_WEIGHTS /= MCC_WEIGHTS.sum()
MCC_MONTANTS = [m[3] for m in MCC_CATEGORIES]

HEURE_WEIGHTS = np.array([
    0.003, 0.002, 0.001, 0.001, 0.002, 0.008,
    0.020, 0.045, 0.070, 0.065, 0.055, 0.060,
    0.075, 0.075, 0.060, 0.050, 0.065, 0.080,
    0.075, 0.065, 0.050, 0.035, 0.020, 0.013,
], dtype=float)
HEURE_WEIGHTS /= HEURE_WEIGHTS.sum()

FRAUD_TYPES   = ["card_not_present", "stolen_card", "account_takeover",
                 "social_engineering", "skimming", "friendly_fraud"]
FRAUD_WEIGHTS = np.array([0.45, 0.20, 0.15, 0.10, 0.07, 0.03], dtype=float)
FRAUD_WEIGHTS /= FRAUD_WEIGHTS.sum()

PAYS_FRAUDE = ["NG", "RO", "UA", "MK", "CN", "IN", "BR", "MX", "PK", "VN"]
PAYS_LEGIT  = ["FR", "FR", "FR", "FR", "FR", "BE", "DE", "ES", "IT", "NL"]


def pseudo(value: str) -> str:
    return hashlib.sha256(f"socgen_v1:{value}".encode()).hexdigest()

def get_conn():
    return psycopg2.connect(DB_URL)


def seed_agences(cur) -> list:
    execute_values(
        cur,
        "INSERT INTO agences (code_agence, nom, region, ville) VALUES %s "
        "ON CONFLICT (code_agence) DO NOTHING",
        AGENCES,
    )
    cur.execute("SELECT id FROM agences ORDER BY id")
    return [r[0] for r in cur.fetchall()]


def seed_clients(cur, n: int, agence_ids: list) -> pd.DataFrame:
    logger.info(f"Génération {n:,} clients...")
    today    = date.today()
    segments = list(SEGMENTS.keys())
    seg_w    = list(SEGMENTS.values())
    rows     = []

    for _ in range(n):
        seg          = str(np.random.choice(segments, p=seg_w))
        r_min, r_max = REVENUS[seg]
        revenu       = round(float(np.random.uniform(r_min, r_max)), 2)
        dob          = today - timedelta(days=int(np.random.randint(25*365, 65*365)))
        anc_mois     = int(np.random.randint(6, 120))
        date_entree  = today - timedelta(days=anc_mois * 30)
        dept         = str(np.random.randint(1, 95)).zfill(2)
        cp           = dept + str(np.random.randint(0, 999)).zfill(3)

        rows.append((
            pseudo(fake.first_name()), pseudo(fake.last_name()),
            dob, cp[:5], dept[:3], seg, "actif",
            int(np.random.choice(agence_ids)),
            revenu, bool(np.random.random() > 0.55),
            "FR", fake.city(), date_entree,
        ))

    # ── CORRECTION : insertion sans RETURNING, récupération séparée ──
    execute_values(
        cur,
        """INSERT INTO clients (
            prenom_hash, nom_hash, date_naissance, code_postal, departement,
            segment, statut, agence_id, revenu_mensuel_net, est_proprietaire,
            pays_residence, ville_principale, date_entree_relation
        ) VALUES %s""",
        rows,
    )

    # Récupération de TOUS les IDs insérés
    cur.execute("""
        SELECT id, revenu_mensuel_net, segment, date_entree_relation
        FROM clients
        ORDER BY id DESC
        LIMIT %s
    """, (n,))
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=["client_id", "revenu", "segment", "date_entree"])
    logger.info(f"  {len(df):,} clients récupérés")
    return df


def seed_comptes(cur, clients_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Génération des comptes...")
    rows = []
    for _, row in clients_df.iterrows():
        solde = round(float(np.random.normal(float(row.revenu) * 1.2,
                                              float(row.revenu) * 0.4)), 2)
        rows.append((int(row.client_id), "courant", "actif",
                     max(solde, 0), float(row.revenu), row.date_entree))

    execute_values(
        cur,
        """INSERT INTO comptes (client_id, type_compte, statut, solde_actuel,
                                plafond_decouvert, date_ouverture)
           VALUES %s""",
        rows,
    )

    # Récupération de TOUS les comptes insérés
    client_ids_list = clients_df["client_id"].tolist()
    cur.execute("""
        SELECT id, client_id FROM comptes
        WHERE client_id = ANY(%s)
    """, (client_ids_list,))
    results     = cur.fetchall()
    compte_df   = pd.DataFrame(results, columns=["compte_id", "client_id"])
    merged      = clients_df.merge(compte_df, on="client_id")
    logger.info(f"  {len(merged):,} comptes récupérés")
    return merged


def seed_transactions(cur, clients_df: pd.DataFrame):
    logger.info("Génération des transactions (vectorisée)...")
    today = date.today()

    nb_tx_par_client = np.random.randint(NB_TX_MIN, NB_TX_MAX, size=len(clients_df))
    total_tx         = int(nb_tx_par_client.sum())
    logger.info(f"  Clients       : {len(clients_df):,}")
    logger.info(f"  Total tx      : {total_tx:,}")

    # Répéter chaque client selon son nb_tx
    client_ids  = np.repeat(clients_df["client_id"].values, nb_tx_par_client)
    compte_ids  = np.repeat(clients_df["compte_id"].values, nb_tx_par_client)
    revenus     = np.repeat(clients_df["revenu"].values.astype(float), nb_tx_par_client)

    # Dates
    jours   = np.random.randint(0, 365, size=total_tx)
    heures  = np.random.choice(24, size=total_tx, p=HEURE_WEIGHTS)
    minutes = np.random.randint(0, 60, size=total_tx)

    # MCC et montants
    mcc_idx  = np.random.choice(len(MCC_CATEGORIES), size=total_tx, p=MCC_WEIGHTS)
    mcc_codes = np.array(MCC_CODES)[mcc_idx]
    mcc_noms  = np.array(MCC_NOMS)[mcc_idx]
    montants  = np.array([
        round(float(np.random.uniform(MCC_MONTANTS[i][0], MCC_MONTANTS[i][1])), 2)
        for i in mcc_idx
    ])

    # Canal
    is_dab     = mcc_codes == "6011"
    is_online  = (np.random.random(total_tx) < 0.30) & ~is_dab
    rand_canal = np.random.random(total_tx)
    canaux     = np.where(is_dab, "dab",
                 np.where(is_online, "online",
                 np.where(rand_canal < 0.45, "sans_contact", "carte_physique")))
    is_contact = canaux == "sans_contact"

    # Géolocalisation
    lat_home    = np.random.uniform(43.0, 51.0, size=len(clients_df))
    lon_home    = np.random.uniform(-2.0,  8.0, size=len(clients_df))
    lat_home_tx = np.repeat(lat_home, nb_tx_par_client)
    lon_home_tx = np.repeat(lon_home, nb_tx_par_client)
    lats        = np.where(is_online, np.nan,
                           lat_home_tx + np.random.uniform(-0.5, 0.5, total_tx))
    lons        = np.where(is_online, np.nan,
                           lon_home_tx + np.random.uniform(-0.5, 0.5, total_tx))
    dists       = np.where(is_online, np.nan,
                           np.abs(np.random.normal(5, 20, total_tx)))
    pays_tx     = np.where(is_online,
                           np.random.choice(PAYS_LEGIT, size=total_tx), "FR")
    ip_ctry     = np.where(is_online,
                           np.random.choice(PAYS_LEGIT, size=total_tx), "")

    auths = np.where(canaux == "dab", "pin",
            np.where(is_online & (np.random.random(total_tx) > 0.3), "3ds",
            np.where(is_contact & (np.random.random(total_tx) > 0.6), "biometric",
            np.where(is_online, "none", "pin"))))

    tx_types = np.where(is_dab, "retrait_dab", "paiement_carte")

    # Fraudes
    is_fraud    = np.random.random(total_tx) < TAUX_FRAUDE
    fraud_types = np.where(is_fraud,
                           np.random.choice(FRAUD_TYPES, size=total_tx, p=FRAUD_WEIGHTS),
                           "")

    fraud_mask_cnp = is_fraud & (fraud_types == "card_not_present")
    fraud_mask_sc  = is_fraud & (fraud_types == "stolen_card")
    fraud_mask_ato = is_fraud & (fraud_types == "account_takeover")

    canaux  = canaux.copy()
    is_online = is_online.copy()

    if fraud_mask_cnp.sum() > 0:
        canaux[fraud_mask_cnp]    = "online"
        is_online[fraud_mask_cnp] = True
        ip_ctry[fraud_mask_cnp]   = np.random.choice(PAYS_FRAUDE, size=fraud_mask_cnp.sum())
        pays_tx[fraud_mask_cnp]   = np.random.choice(PAYS_FRAUDE + ["FR","FR"],
                                                       size=fraud_mask_cnp.sum())
        auths[fraud_mask_cnp]     = "none"
        montants[fraud_mask_cnp]  = np.random.uniform(50, 1500, size=fraud_mask_cnp.sum())

    if fraud_mask_sc.sum() > 0:
        dists[fraud_mask_sc]    = np.random.uniform(20, 300, size=fraud_mask_sc.sum())
        montants[fraud_mask_sc] = np.random.uniform(100, 500, size=fraud_mask_sc.sum())

    if fraud_mask_ato.sum() > 0:
        canaux[fraud_mask_ato]    = "online"
        is_online[fraud_mask_ato] = True
        ip_ctry[fraud_mask_ato]   = np.random.choice(PAYS_FRAUDE, size=fraud_mask_ato.sum())
        auths[fraud_mask_ato]     = "none"

    nb_fraudes = int(is_fraud.sum())
    logger.info(f"  Fraudes       : {nb_fraudes:,} ({nb_fraudes/total_tx*100:.2f}%)")

    # Insertion par batch
    BATCH      = 5000
    tx_batch   = []
    lb_batch   = []

    for i in range(total_tx):
        tx_date  = today - timedelta(days=int(jours[i]))
        tx_dt    = datetime(tx_date.year, tx_date.month, tx_date.day,
                            int(heures[i]), int(minutes[i]))
        lat_val  = None if np.isnan(lats[i])  else round(float(lats[i]),  6)
        lon_val  = None if np.isnan(lons[i])  else round(float(lons[i]),  6)
        dist_val = None if np.isnan(dists[i]) else round(float(dists[i]), 1)
        ip_val   = None if ip_ctry[i] == "" else str(ip_ctry[i])

        tx_batch.append((
            int(compte_ids[i]), int(client_ids[i]),
            round(float(montants[i]), 2), "EUR",
            str(tx_types[i]), str(canaux[i]),
            f"TERM{np.random.randint(1000,9999)}" if not is_online[i] else None,
            str(mcc_codes[i]), str(mcc_noms[i]),
            str(pays_tx[i]),
            lat_val, lon_val, dist_val,
            bool(is_online[i]), bool(is_contact[i]),
            ip_val, str(auths[i]),
            None, tx_dt,
        ))
        lb_batch.append((
            int(client_ids[i]),
            bool(is_fraud[i]),
            str(fraud_types[i]) if fraud_types[i] else None,
            round(float(montants[i]), 2) if is_fraud[i] else None,
        ))

        if len(tx_batch) >= BATCH or i == total_tx - 1:
            execute_values(
                cur,
                """INSERT INTO transactions (
                    compte_id, client_id, montant, devise,
                    type_transaction, canal,
                    terminal_id, mcc_code, nom_commercant,
                    pays_transaction, latitude, longitude, distance_domicile_km,
                    is_online, is_contactless, ip_country, authentification,
                    solde_apres, date_transaction
                ) VALUES %s RETURNING id, client_id""",
                tx_batch, page_size=len(tx_batch),
            )
            tx_ids = cur.fetchall()

            label_rows = []
            for (tx_id, cid), (_, is_f, ft, fa) in zip(tx_ids, lb_batch):
                reported  = (datetime.now() - timedelta(days=int(np.random.randint(1, 30)))
                             if is_f else None)
                confirmed = (reported + timedelta(days=int(np.random.randint(1, 10)))
                             if reported else None)
                label_rows.append((
                    tx_id, cid, is_f, ft, fa,
                    reported, confirmed,
                    is_f and np.random.random() > 0.2,
                ))

            execute_values(
                cur,
                """INSERT INTO fraud_labels (
                    transaction_id, client_id, is_fraud,
                    fraud_type, fraud_amount,
                    fraud_reported_at, fraud_confirmed_at, chargeback_raised
                ) VALUES %s""",
                label_rows, page_size=len(label_rows),
            )
            tx_batch.clear()
            lb_batch.clear()

    return total_tx, nb_fraudes


def seed_card_blocks(cur, clients_df: pd.DataFrame):
    motifs = ["fraude_suspectee", "perte", "vol", "opposition"]
    rows   = []
    for _, row in clients_df.iterrows():
        if np.random.random() > 0.02:
            continue
        motif  = str(np.random.choice(motifs, p=[0.5, 0.2, 0.2, 0.1]))
        date_b = datetime.now() - timedelta(days=int(np.random.randint(1, 180)))
        date_d = (date_b + timedelta(days=int(np.random.randint(1, 30)))) \
                 if np.random.random() > 0.4 else None
        rows.append((int(row.client_id), int(row.compte_id), motif, date_b, date_d))
    if rows:
        execute_values(
            cur,
            "INSERT INTO card_blocks (client_id, compte_id, motif, date_blocage, date_deblocage) VALUES %s",
            rows,
        )


def main(n_clients: int):
    console.rule("[bold red]SocGen Fraud Detection — Seed[/bold red]")
    console.print(f"  Clients     : [bold]{n_clients:,}[/bold]")
    console.print(f"  Taux fraude : [bold]{TAUX_FRAUDE*100:.1f}%[/bold]")
    console.print(f"  Tx/client   : [bold]{NB_TX_MIN}–{NB_TX_MAX}[/bold]")

    conn = get_conn()
    conn.autocommit = False
    cur  = conn.cursor()

    with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                  BarColumn(), TimeElapsedColumn(), console=console) as progress:
        t = progress.add_task("Démarrage...", total=None)

        progress.update(t, description="[cyan]Agences...")
        agence_ids = seed_agences(cur)
        conn.commit()

        progress.update(t, description="[cyan]Clients...")
        clients_df = seed_clients(cur, n_clients, agence_ids)
        conn.commit()

        progress.update(t, description="[cyan]Comptes...")
        clients_df = seed_comptes(cur, clients_df)
        conn.commit()

        progress.update(t, description="[cyan]Transactions + labels fraude...")
        nb_total, nb_fraudes = seed_transactions(cur, clients_df)
        conn.commit()

        progress.update(t, description="[cyan]Blocages carte...")
        seed_card_blocks(cur, clients_df)
        conn.commit()

    cur.close()
    conn.close()

    taux_reel = nb_fraudes / nb_total * 100 if nb_total > 0 else 0
    console.rule("[bold green]Seed terminé[/bold green]")
    console.print(f"  Clients      : [bold]{n_clients:,}[/bold]")
    console.print(f"  Transactions : [bold]{nb_total:,}[/bold]")
    console.print(f"  Fraudes      : [bold red]{nb_fraudes:,}[/bold red] ({taux_reel:.2f}%)")
    console.print(f"\n  [green]Adminer : http://localhost:8080[/green]")
    console.print(f"  [green]MLflow  : http://localhost:5000[/green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clients", type=int, default=20_000)
    args = parser.parse_args()
    main(args.clients)