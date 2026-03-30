from fastapi import APIRouter
import api.main as app_state

router = APIRouter()

@router.get("/health")
def health():
    return {
        "status":  "ok",
        "model":   "loaded" if app_state.MODEL else "not loaded",
        "run_id":  app_state.MODEL["run_id"] if app_state.MODEL else None,
        "auc":     app_state.MODEL["auc"]    if app_state.MODEL else None,
    }