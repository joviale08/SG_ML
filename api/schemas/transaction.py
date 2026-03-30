from pydantic import BaseModel, Field
from typing import Optional


class TransactionInput(BaseModel):
    """Transaction bancaire à analyser."""

    # Transaction
    montant:              float = Field(..., gt=0, description="Montant en EUR")
    canal:                str   = Field(..., description="carte_physique | online | dab | sans_contact")
    type_transaction:     str   = Field(default="paiement_carte")
    mcc_code:             str   = Field(..., description="Merchant Category Code")
    pays_transaction:     str   = Field(default="FR")
    is_online:            bool  = Field(default=False)
    is_contactless:       bool  = Field(default=False)
    authentification:     str   = Field(default="pin")
    distance_domicile_km: float = Field(default=0.0)

    # Temporel
    heure:                int   = Field(..., ge=0, le=23)
    jour_semaine:         int   = Field(..., ge=0, le=6)
    mois:                 int   = Field(..., ge=1, le=12)
    is_nuit:              bool  = Field(default=False)
    is_weekend:           bool  = Field(default=False)

    # Vélocité
    nb_tx_1h:             int   = Field(default=0)
    nb_tx_24h:            int   = Field(default=0)
    montant_cumul_1h:     float = Field(default=0.0)
    montant_cumul_24h:    float = Field(default=0.0)
    nb_pays_24h:          int   = Field(default=1)
    nb_terminaux_24h:     int   = Field(default=1)

    # Statistiques
    ratio_montant_vs_moyenne: float = Field(default=1.0)
    zscore_montant:           float = Field(default=0.0)
    ratio_montant_revenu:     float = Field(default=0.0)

    # Client
    revenu_mensuel_net:   float = Field(default=2500.0)
    anciennete_mois:      int   = Field(default=12)
    age_client:           int   = Field(default=35)
    segment:              str   = Field(default="particulier")
    pays_inhabituel:      bool  = Field(default=False)

    class Config:
        json_schema_extra = {
            "example": {
                "montant":              245.00,
                "canal":                "online",
                "type_transaction":     "paiement_carte",
                "mcc_code":             "5945",
                "pays_transaction":     "NG",
                "is_online":            True,
                "is_contactless":       False,
                "authentification":     "none",
                "distance_domicile_km": 0.0,
                "heure":                2,
                "jour_semaine":         1,
                "mois":                 3,
                "is_nuit":              True,
                "is_weekend":           False,
                "nb_tx_1h":             4,
                "nb_tx_24h":            8,
                "montant_cumul_1h":     890.0,
                "montant_cumul_24h":    1200.0,
                "nb_pays_24h":          3,
                "nb_terminaux_24h":     5,
                "ratio_montant_vs_moyenne": 3.2,
                "zscore_montant":       2.8,
                "ratio_montant_revenu": 0.09,
                "revenu_mensuel_net":   2800.0,
                "anciennete_mois":      24,
                "age_client":           32,
                "segment":              "particulier",
                "pays_inhabituel":      True,
            }
        }


class FraudPrediction(BaseModel):
    """Réponse de l'API."""
    is_fraud:         bool
    score:            float
    threshold:        float
    risk_level:       str
    top_features:     list
    model_version:    str
    processing_ms:    float