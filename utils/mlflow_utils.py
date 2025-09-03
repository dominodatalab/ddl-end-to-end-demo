import os
import mlflow
import mlflow.sklearn
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from mlflow import MlflowClient
from mlflow.entities import Run
from mlflow.entities.model_registry import ModelVersion
from mlflow.models.model import ModelInfo
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from mlflow.exceptions import RestException
from typing import Optional, Iterable, Union
import numpy as np
import pandas as pd
def ensure_mlflow_experiment(experiment_name: str) -> int:
    """
    Ensure an MLflow experiment exists and set it as current.

    If an experiment with `experiment_name` does not exist, create it. In both cases,
    set the active experiment so subsequent runs attach correctly.

    Parameters
    ----------
    experiment_name : str
        The MLflow experiment name.

    Returns
    -------
    str
        The experiment ID.

    Raises
    ------
    RuntimeError
        If the experiment lookup/creation fails.

    Notes
    -----
    - The MLflow tracking URI and token are pre-configured in Domino
    """
    try:
        exp = mlflow.get_experiment_by_name(experiment_name)
        if exp is None:
            exp_id = mlflow.create_experiment(
                experiment_name
            )
        else:
            exp_id = exp.experiment_id
        mlflow.set_experiment(experiment_name)
        return exp_id
    except Exception as e:
        raise RuntimeError(f"Failed to ensure experiment {experiment_name}: {e}")

def ensure_registered_model(model_name: str):
    """
    Ensure a registered model exists; return its metadata.

    - If the model exists, returns it.
    - If it doesn't, creates it and returns the created model.

    You can control registry/tracking destinations via env vars:
      MLFLOW_TRACKING_URI, MLFLOW_REGISTRY_URI
    """
    client = MlflowClient()

    # Prefer "get-first" to avoid noisy create-conflict logs.
    try:
        return client.get_registered_model(model_name)
    except RestException as e:
        # If it's missing, create it; otherwise, re-raise.
        code = getattr(e, "error_code", None)
        msg = str(e)
        if code in {"RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"} or "does not exist" in msg.lower():
            return client.create_registered_model(model_name)
        raise

def register_model_version(
    model_name: str,
    model_desc: str,
    model_info: ModelInfo,
    run: Run,
    ) -> ModelVersion:
    """
    Register a new model version from a logged model.

    Args:
        model_name: Registered model name.
        model_desc: Description for this version.
        model_info: Result of mlflow.<flavor>.log_model(...), has .model_uri.
        run: The MLflow Run object (uses run.info.run_id).

    Returns:
        The created ModelVersion.
    """
    client = MlflowClient()
    source_uri = RunsArtifactRepository.get_underlying_uri(model_info.model_uri)
    return client.create_model_version(
        name=model_name,
        source=source_uri,
        run_id=run.info.run_id,
        description=model_desc,
    )
def load_registered_model_version(model_name: str, version: Union[int, str]) -> mlflow.pyfunc.PyFuncModel:
    """
    Load a registered model *version* as a PyFunc model.
    Works regardless of how the model was trained/logged (XGBoost flavor included).

    Example URI: models:/my_model/3
    """
    uri = f"models:/{model_name}/{version}"
    return mlflow.pyfunc.load_model(uri)


def predict_with_pyfunc(model: mlflow.pyfunc.PyFuncModel,
                        X: Union[pd.DataFrame, np.ndarray, Iterable[Iterable[float]]]) -> np.ndarray:
    """
    Run predictions using a PyFunc model.
    Accepts pandas DataFrame or numpy-like 2D structure.
    """
    if not isinstance(X, pd.DataFrame):
        X = pd.DataFrame(X)
    print(X)
    y_pred = model.predict(X)
    # Ensure numpy array
    return np.asarray(y_pred)