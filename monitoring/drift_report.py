"""
Société Générale — Fraud Detection
Monitoring et détection de drift avec Evidently AI 0.7.x
Usage : python monitoring/drift_report.py
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine

from evidently import Dataset, DataDefinition
from evidently import Report
from evidently.presets import DataDriftPreset

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "postgresql://socgen:socgen_secret@localhost:5432/socgen_fraud")

FEATURES_NUM = [
    "montant", "distance_domicile_km",
    "nb_tx_1h", "nb_tx_24h", "montant_cumul_1h", "montant_cumul_24h",
    "nb_pays_24h", "nb_terminaux_24h",
    "ratio_montant_vs_moyenne", "zscore_montant", "ratio_montant_revenu",
    "revenu_mensuel_net", "anciennete_mois", "age_client",
    "heure", "jour_semaine", "mois",
]

FEATURES_CAT = [
    "is_online", "is_contactless", "is_nuit", "is_weekend", "pays_inhabituel",
]


def load_data() -> pd.DataFrame:
    logger.info("Chargement des données...")
    engine = create_engine(DB_URL)
    df = pd.read_sql(
        "SELECT * FROM dbt_socgen_mart.mart_fraud_detection ORDER BY date_transaction",
        engine
    )
    logger.info(f"  {len(df):,} transactions chargées")
    return df


def split_reference_current(df: pd.DataFrame):
    split_idx = int(len(df) * 0.80)
    reference = df.iloc[:split_idx][FEATURES_NUM + FEATURES_CAT + ["is_fraud"]].copy()
    current   = df.iloc[split_idx:][FEATURES_NUM + FEATURES_CAT + ["is_fraud"]].copy()
    logger.info(f"  Référence : {len(reference):,} | Courant : {len(current):,}")
    return reference, current


def make_dataset(df: pd.DataFrame) -> Dataset:
    data_def = DataDefinition(
        numerical_columns=FEATURES_NUM,
        categorical_columns=FEATURES_CAT,
    )
    return Dataset.from_pandas(df, data_definition=data_def)


def generate_drift_report(reference: pd.DataFrame, current: pd.DataFrame, label: str):
    logger.info(f"Génération rapport drift — {label}...")

    ref_ds = make_dataset(reference)
    cur_ds = make_dataset(current)

    report = Report([DataDriftPreset()])
    result = report.run(ref_ds, cur_ds)

    os.makedirs("monitoring/reports", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path      = f"monitoring/reports/drift_{label}_{timestamp}.html"
    result.save_html(path)
    logger.success(f"Rapport sauvegardé : {path}")

    return path


def simulate_drift(current: pd.DataFrame) -> pd.DataFrame:
    logger.warning("Simulation dérive...")
    df = current.copy()

    # Dérive sur TOUTES les transactions — pas seulement la nuit
    df["nb_tx_1h"]                = df["nb_tx_1h"] * 5 + np.random.randint(3, 8, size=len(df))
    df["zscore_montant"]          = df["zscore_montant"] + np.random.uniform(2.5, 4.0, size=len(df))
    df["ratio_montant_vs_moyenne"]= df["ratio_montant_vs_moyenne"] * 3.5
    df["pays_inhabituel"]         = np.random.choice([0, 1], size=len(df), p=[0.3, 0.7])
    df["montant_cumul_1h"]        = df["montant_cumul_1h"] * 4
    df["nb_tx_24h"]               = df["nb_tx_24h"] * 3

    logger.warning(f"  Dérive appliquée sur {len(df):,} transactions")
    return df


def main():
    logger.info("SocGen — Monitoring Evidently AI 0.7.x")

    df = load_data()
    reference, current = split_reference_current(df)

    logger.info("\n[1/2] Rapport données normales")
    path1 = generate_drift_report(reference, current, "normal")

    logger.info("\n[2/2] Rapport données avec dérive simulée")
    current_drift = simulate_drift(current)
    path2 = generate_drift_report(reference, current_drift, "derive")

    logger.info("\nRÉSUMÉ")
    logger.info(f"  Rapport normal : {path1}")
    logger.info(f"  Rapport dérive : {path2}")
    logger.info(f"  Ouvrez les fichiers HTML dans votre navigateur")


if __name__ == "__main__":
    main()