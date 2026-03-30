import time
import pandas as pd
import shap
from fastapi import APIRouter, HTTPException
import api.main as app_state
from api.schemas.transaction import TransactionInput, FraudPrediction

router = APIRouter()

FEATURES = [
    "montant", "is_online", "is_contactless", "is_nuit", "is_weekend",
    "heure", "jour_semaine", "mois", "distance_domicile_km",
    "nb_tx_1h", "nb_tx_24h", "montant_cumul_1h", "montant_cumul_24h",
    "nb_pays_24h", "nb_terminaux_24h",
    "ratio_montant_vs_moyenne", "zscore_montant", "ratio_montant_revenu",
    "revenu_mensuel_net", "anciennete_mois", "age_client", "pays_inhabituel",
    "canal_enc", "authentification_enc", "segment_enc", "mcc_code_enc",
]

CANAL_VALS = ["carte_physique", "online", "dab", "sans_contact", "virement", "inconnu"]
AUTH_VALS  = ["pin", "3ds", "biometric", "none", "inconnu"]
SEG_VALS   = ["particulier", "premium", "private", "professionnel", "inconnu"]
MCC_VALS   = ["5411", "5812", "5541", "5912", "5311", "4111", "5999",
              "7011", "4722", "5945", "5734", "7922", "5651", "6011", "inconnu"]

def encode(value: str, vocab: list) -> int:
    value = str(value).strip().lower()
    vocab_lower = [v.lower() for v in vocab]
    return vocab_lower.index(value) if value in vocab_lower else len(vocab) - 1


@router.post("/predict", response_model=FraudPrediction)
def predict(tx: TransactionInput):
    if app_state.MODEL is None:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    start = time.time()

    row = {
        "montant":                  tx.montant,
        "is_online":                int(tx.is_online),
        "is_contactless":           int(tx.is_contactless),
        "is_nuit":                  int(tx.is_nuit),
        "is_weekend":               int(tx.is_weekend),
        "heure":                    tx.heure,
        "jour_semaine":             tx.jour_semaine,
        "mois":                     tx.mois,
        "distance_domicile_km":     tx.distance_domicile_km,
        "nb_tx_1h":                 tx.nb_tx_1h,
        "nb_tx_24h":                tx.nb_tx_24h,
        "montant_cumul_1h":         tx.montant_cumul_1h,
        "montant_cumul_24h":        tx.montant_cumul_24h,
        "nb_pays_24h":              tx.nb_pays_24h,
        "nb_terminaux_24h":         tx.nb_terminaux_24h,
        "ratio_montant_vs_moyenne": tx.ratio_montant_vs_moyenne,
        "zscore_montant":           tx.zscore_montant,
        "ratio_montant_revenu":     tx.ratio_montant_revenu,
        "revenu_mensuel_net":       tx.revenu_mensuel_net,
        "anciennete_mois":          tx.anciennete_mois,
        "age_client":               tx.age_client,
        "pays_inhabituel":          int(tx.pays_inhabituel),
        "canal_enc":                encode(tx.canal,            CANAL_VALS),
        "authentification_enc":     encode(tx.authentification, AUTH_VALS),
        "segment_enc":              encode(tx.segment,          SEG_VALS),
        "mcc_code_enc":             encode(tx.mcc_code,         MCC_VALS),
    }

    X        = pd.DataFrame([row])[FEATURES]
    score    = float(app_state.MODEL["model"].predict_proba(X)[0][1])
    threshold = app_state.MODEL["threshold"]
    is_fraud = score >= threshold

    if score < 0.3:
        risk_level = "faible"
    elif score < 0.6:
        risk_level = "moyen"
    elif score < threshold:
        risk_level = "élevé"
    else:
        risk_level = "fraude détectée"

    try:
        explainer   = shap.TreeExplainer(app_state.MODEL["model"])
        shap_values = explainer.shap_values(X)
        shap_arr    = shap_values[0] if isinstance(shap_values, list) else shap_values[0]
        top_features = sorted(
            [{"feature": f, "impact": round(float(v), 4)}
             for f, v in zip(FEATURES, shap_arr)],
            key=lambda x: abs(x["impact"]),
            reverse=True
        )[:5]
    except Exception:
        top_features = []

    processing_ms = round((time.time() - start) * 1000, 2)

    return FraudPrediction(
        is_fraud=is_fraud,
        score=round(score, 4),
        threshold=round(threshold, 4),
        risk_level=risk_level,
        top_features=top_features,
        model_version=app_state.MODEL["run_id"][:8],
        processing_ms=processing_ms,
    )