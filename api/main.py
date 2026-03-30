"""
Société Générale — Fraud Detection API
Usage : uvicorn api.main:app --reload --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import mlflow.lightgbm
import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Chargement du modèle au démarrage

MODEL = None

def load_model():
    global MODEL
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "./mlruns")
    mlflow.set_tracking_uri(tracking_uri)

    client   = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name("socgen-fraud-detection")
    if not experiment:
        raise RuntimeError("Expérience MLflow introuvable")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.auc_roc DESC"],
        max_results=1,
    )
    if not runs:
        raise RuntimeError("Aucun run MLflow trouvé")

    best_run   = runs[0]
    run_id     = best_run.info.run_id
    threshold  = best_run.data.params.get("decision_threshold", "0.5")
    model_uri  = f"runs:/{run_id}/lightgbm_model"

    logger.info(f"Chargement modèle run_id={run_id}")
    MODEL = {
        "model":     mlflow.lightgbm.load_model(model_uri),
        "threshold": float(threshold),
        "run_id":    run_id,
        "auc":       best_run.data.metrics.get("auc_roc", 0),
    }
    logger.success(f"Modèle chargé — AUC={MODEL['auc']:.4f} seuil={MODEL['threshold']:.4f}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
    yield

# Application 

app = FastAPI(
    title="SocGen Fraud Detection API",
    description="API de détection de fraude carte bancaire — Société Générale",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

from api.routers.predict import router as predict_router
from api.routers.health  import router as health_router
app.include_router(health_router,  tags=["Health"])
app.include_router(predict_router, tags=["Prediction"], prefix="/api/v1")