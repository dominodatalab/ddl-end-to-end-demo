import os
import mlflow

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