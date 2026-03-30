"""
Société Générale — Fraud Detection
Entraînement du modèle LightGBM avec tracking MLflow
Usage : python ml/train.py
"""

import os
import warnings
warnings.filterwarnings("ignore")

import mlflow
import mlflow.lightgbm
import lightgbm as lgb
import numpy as np
import pandas as pd
import shap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from loguru import logger
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    f1_score, precision_score, recall_score,
    precision_recall_curve
)
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE
from sqlalchemy import create_engine

load_dotenv()

DB_URL       = os.getenv("DATABASE_URL", "postgresql://socgen:socgen_secret@localhost:5432/socgen_fraud")
MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI", "./mlruns")
EXPERIMENT   = "socgen-fraud-detection"
RANDOM_STATE = 42

FEATURES = [
    "montant", "is_online", "is_contactless", "is_nuit", "is_weekend",
    "heure", "jour_semaine", "mois", "distance_domicile_km",
    "nb_tx_1h", "nb_tx_24h", "montant_cumul_1h", "montant_cumul_24h",
    "nb_pays_24h", "nb_terminaux_24h",
    "ratio_montant_vs_moyenne", "zscore_montant", "ratio_montant_revenu",
    "revenu_mensuel_net", "anciennete_mois", "age_client", "pays_inhabituel",
    "canal_enc", "authentification_enc", "segment_enc", "mcc_code_enc",
]

TARGET = "is_fraud"


def load_data() -> pd.DataFrame:
    logger.info("Chargement des données depuis mart_fraud_detection...")
    engine = create_engine(DB_URL)
    df = pd.read_sql(
        "SELECT * FROM dbt_socgen_mart.mart_fraud_detection",
        engine
    )
    logger.info(f"  {len(df):,} transactions chargées")
    logger.info(f"  Fraudes : {df['is_fraud'].sum():,} ({df['is_fraud'].mean()*100:.2f}%)")
    return df


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Préparation des features...")
    for col, new_col in [
        ("canal",           "canal_enc"),
        ("authentification","authentification_enc"),
        ("segment",         "segment_enc"),
        ("mcc_code",        "mcc_code_enc"),
    ]:
        le = LabelEncoder()
        df[new_col] = le.fit_transform(df[col].fillna("inconnu").astype(str))

    for col in FEATURES:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df


def train(df: pd.DataFrame):
    logger.info("Démarrage de l'entraînement...")

    X = df[FEATURES]
    y = df[TARGET].astype(int)

    df_sorted  = df.sort_values("date_transaction")
    split_idx  = int(len(df_sorted) * 0.80)
    train_idx  = df_sorted.index[:split_idx]
    test_idx   = df_sorted.index[split_idx:]

    X_train, X_test = X.loc[train_idx], X.loc[test_idx]
    y_train, y_test = y.loc[train_idx], y.loc[test_idx]

    logger.info(f"  Train : {len(X_train):,} | Test : {len(X_test):,}")
    logger.info(f"  Fraudes train : {y_train.sum():,} | Fraudes test : {y_test.sum():,}")

    logger.info("Application SMOTE...")
    k     = min(5, max(1, y_train.sum() - 1))
    smote = SMOTE(random_state=RANDOM_STATE, k_neighbors=k)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)
    logger.info(f"  Après SMOTE : {len(X_train_res):,} samples")

    params = {
        "objective":         "binary",
        "metric":            "auc",
        "boosting_type":     "gbdt",
        "num_leaves":        63,
        "learning_rate":     0.05,
        "n_estimators":      500,
        "min_child_samples": 20,
        "feature_fraction":  0.8,
        "bagging_fraction":  0.8,
        "bagging_freq":      5,
        "reg_alpha":         0.1,
        "reg_lambda":        0.1,
        "random_state":      RANDOM_STATE,
        "n_jobs":            -1,
        "verbose":           -1,
    }

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    with mlflow.start_run(run_name="lightgbm-smote-v4"):

        mlflow.log_params(params)
        mlflow.log_param("smote",           True)
        mlflow.log_param("split_type",      "temporal_80_20")
        mlflow.log_param("n_features",      len(FEATURES))
        mlflow.log_param("train_size",      len(X_train_res))
        mlflow.log_param("test_size",       len(X_test))
        mlflow.log_param("fraud_rate",      round(float(y_train.mean()), 4))
        mlflow.log_param("n_fraudes_train", int(y_train.sum()))
        mlflow.log_param("n_fraudes_test",  int(y_test.sum()))

        logger.info("Entraînement LightGBM...")
        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train_res, y_train_res,
            eval_set=[(X_test, y_test)],
            callbacks=[
                lgb.early_stopping(50, verbose=False),
                lgb.log_evaluation(100)
            ]
        )

        y_pred_proba = model.predict_proba(X_test)[:, 1]

        precisions, recalls, thresholds = precision_recall_curve(y_test, y_pred_proba)
        f1_scores      = 2 * precisions * recalls / (precisions + recalls + 1e-8)
        best_threshold = float(thresholds[f1_scores[:-1].argmax()])
        best_threshold = max(best_threshold, 0.3)
        y_pred         = (y_pred_proba >= best_threshold).astype(int)

        mlflow.log_param("decision_threshold", round(best_threshold, 4))

        auc       = roc_auc_score(y_test, y_pred_proba)
        ap        = average_precision_score(y_test, y_pred_proba)
        f1        = f1_score(y_test, y_pred, zero_division=0)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall    = recall_score(y_test, y_pred, zero_division=0)
        gini      = 2 * auc - 1

        logger.info(f"  AUC-ROC   : {auc:.4f}")
        logger.info(f"  Gini      : {gini:.4f}")
        logger.info(f"  Avg Prec  : {ap:.4f}")
        logger.info(f"  F1        : {f1:.4f}")
        logger.info(f"  Precision : {precision:.4f}")
        logger.info(f"  Recall    : {recall:.4f}")
        logger.info(f"  Seuil     : {best_threshold:.4f}")

        mlflow.log_metric("auc_roc",           auc)
        mlflow.log_metric("gini",              gini)
        mlflow.log_metric("average_precision", ap)
        mlflow.log_metric("f1_score",          f1)
        mlflow.log_metric("precision",         precision)
        mlflow.log_metric("recall",            recall)

        # ── Feature importance ────────────────────────────────────────────
        fi = pd.DataFrame({
            "feature":    FEATURES,
            "importance": model.feature_importances_
        }).sort_values("importance", ascending=False)

        fig_fi, ax = plt.subplots(figsize=(10, 8))
        fi.head(20).plot.barh(x="feature", y="importance", ax=ax, color="steelblue")
        ax.set_title("Top 20 features — LightGBM Fraud Detection SocGen")
        ax.invert_yaxis()
        plt.tight_layout()
        mlflow.log_figure(fig_fi, "plots/feature_importance.png")
        plt.close(fig_fi)
        logger.info("  Feature importance sauvegardée")

        # ── SHAP values ───────────────────────────────────────────────────
        logger.info("Calcul SHAP values...")
        sample_size = min(300, len(X_test))
        X_sample    = X_test.sample(sample_size, random_state=RANDOM_STATE)
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)

        fig_shap, ax_shap = plt.subplots(figsize=(10, 8))
        shap.summary_plot(
            shap_values, X_sample,
            feature_names=FEATURES,
            show=False,
            plot_size=None
        )
        plt.tight_layout()
        mlflow.log_figure(plt.gcf(), "plots/shap_summary.png")
        plt.close("all")
        logger.info("  SHAP summary sauvegardé")

        # ── Sauvegarde modèle ─────────────────────────────────────────────
        mlflow.lightgbm.log_model(model, "lightgbm_model")

        run_id = mlflow.active_run().info.run_id
        logger.success(f"Run MLflow : {run_id}")
        logger.success(f"MLflow UI  : http://localhost:5001")

    return model, auc


if __name__ == "__main__":
    df         = load_data()
    df         = prepare_features(df)
    model, auc = train(df)
    logger.success(f"Entraînement terminé — AUC : {auc:.4f}")