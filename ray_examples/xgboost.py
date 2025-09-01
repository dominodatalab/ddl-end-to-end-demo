import os
import math
import pandas as pd
import xgboost as xgb
import pyarrow as pa
import pyarrow.dataset as pds

from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split

import mlflow
import mlflow.xgboost as mlflow_xgb
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature

import ray
from ray import tune
from ray.air import RunConfig, ScalingConfig
from ray.data import read_parquet
from ray.air.integrations.mlflow import MLflowLoggerCallback
from ray.train.xgboost import XGBoostTrainer
from ray.tune.search.hyperopt import HyperOptSearch
from ray.tune.schedulers import ASHAScheduler

try:
    from ray.tune.callback import Callback      # Ray >= 2.6
except ImportError:
    from ray.tune.callbacks import Callback     # Older Ray
from typing import Dict
from utils import mlflow_utils

import hydra
from omegaconf import DictConfig
from omegaconf import OmegaConf


def ensure_ray_connected(ray_envs: Dict[str,str], ray_ns:str):
    if ray.is_initialized():
        return
    # Reconnect to the running cluster (prefers ray:// if present)
    addr = None
    if "RAY_HEAD_SERVICE_HOST" in os.environ and "RAY_HEAD_SERVICE_PORT" in os.environ:
        addr = f"ray://{os.environ['RAY_HEAD_SERVICE_HOST']}:{os.environ['RAY_HEAD_SERVICE_PORT']}"
    ray.init(
        address=addr or "auto",
        runtime_env={"env_vars": ray_envs},   # same env you used earlier
        namespace=ray_ns,
        ignore_reinit_error=True,
    )

def _s3p(root: str, sub: str) -> str:
    """Safe join for S3/posix URIs."""
    return f"{root.rstrip('/')}/{sub.lstrip('/')}"


def read_parquet_to_pandas(uri: str, columns=None, limit: int | None = None) -> pd.DataFrame:
    """
    Robust Parquet→pandas loader that bypasses Ray Data.
    Works with local paths and s3:// (PyArrow uses AWS_* env vars / IRSA).
    """
    ds = pds.dataset(uri.rstrip("/"), format="parquet")
    if limit is None:
        return ds.to_table(columns=columns).to_pandas()

    # Respect limit across files/row groups
    scanner = pds.Scanner.from_dataset(ds, columns=columns)
    batches, rows = [], 0
    for b in scanner.to_batches():
        batches.append(b)
        rows += len(b)
        if rows >= limit:
            return pa.Table.from_batches(batches)[:limit].to_pandas()
    return pa.Table.from_batches(batches).to_pandas()


def main(experiment_name:str,data_dir: str,num_workers: int = 4, cpus_per_worker: int = 1,  DEV_FAST: bool = False):
    """
    Quick knobs:
      - num_workers * cpus_per_worker = CPUs per trial.
      - trainer_resources={"CPU":0} so the driver doesn't steal a core.
      - PACK placement to keep trials tight.
      - max_concurrent_trials caps parallel trials.
      - num_boost_round / early_stopping_rounds control trial length.
      - nthread = cpus_per_worker to avoid oversubscription.
    """

    exp_id = mlflow_utils.ensure_mlflow_experiment(experiment_name)
    
    # Storage: local for dev, S3/your env otherwise
    RUN_STORAGE = os.environ.get("RAY_AIR_STORAGE",f"{data_dir}/air/xgb")
    TUNER_STORAGE = "/tmp/air-dev" if DEV_FAST else RUN_STORAGE
    #FINAL_STORAGE = "/mnt/data/ddl-end-to-end-demo/air/final_fit" if DEV_FAST else RUN_STORAGE
    FINAL_STORAGE = RUN_STORAGE
    # Sanity: workers see IRSA env?
    @ray.remote
    def _peek():
        import os
        return {
            "ROLE": bool(os.environ.get("AWS_ROLE_ARN")),
            "TOKEN_FILE": os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE"),
            "REGION": os.environ.get("AWS_REGION"),
        }
    print("Worker env peek:", ray.get(_peek.remote()))

    # MLflow (experiment + parent run)
    CLUSTER_TRACKING_URI = os.environ["CLUSTER_MLFLOW_TRACKING_URI"]
    
    
    client = MlflowClient()


    parent = client.create_run(
        experiment_id=exp_id,
        tags={"mlflow.runName": "xgb_parent", "role": "tune_parent"},
    )
    parent_run_id = parent.info.run_id
    print("Parent run id:", parent_run_id)

    # Data (Ray Datasets for training/val)
    train_ds = read_parquet(_s3p(data_dir, "train"), parallelism=num_workers)
    val_ds   = read_parquet(_s3p(data_dir, "val"),   parallelism=num_workers)
    test_ds  = read_parquet(_s3p(data_dir, "test"),  parallelism=num_workers)
    print("Schema:", train_ds.schema())

    # Label + features
    label_col = "median_house_value"
    feature_cols = [c for c in train_ds.schema().names if c != label_col]
    keep = feature_cols + [label_col]
    train_ds = train_ds.select_columns(keep)
    val_ds   = val_ds.select_columns(keep)

    # DEV: trim Ray Datasets used for training; eval will bypass Ray entirely
    if DEV_FAST:
        train_ds = train_ds.limit(5_000)
        val_ds   = val_ds.limit(2_000)

    # --- Build test DataFrame without Ray (avoids 'Global node is not initialized') ---
    test_uri = _s3p(data_dir, "test")
    test_pdf = read_parquet_to_pandas(
        test_uri, columns=keep, limit=2_000 if DEV_FAST else None
    )

    # Search space
    param_space = {
        "params": {
            "objective": "reg:squarederror",
            "tree_method": "hist",
            "eval_metric": "rmse",
            "eta": tune.loguniform(1e-3, 3e-1),
            "max_depth": tune.randint(4, 12),
            "min_child_weight": tune.loguniform(1e-2, 10),
            "subsample": tune.uniform(0.6, 1.0),
            "colsample_bytree": tune.uniform(0.6, 1.0),
            "lambda": tune.loguniform(1e-3, 10),
            "alpha": tune.loguniform(1e-3, 10),
        },
        "num_boost_round": 300,
        "early_stopping_rounds": 20,
    }

    # Dev shortcuts
    if DEV_FAST:
        param_space["num_boost_round"] = 20
        param_space["early_stopping_rounds"] = 5
        num_workers = 1
        cpus_per_worker = 1
        NUM_SAMPLES = 5
        MAX_CONCURRENT = 3
        SAVE_ARTIFACTS = True
    else:
        NUM_SAMPLES = 30
        MAX_CONCURRENT = 3
        SAVE_ARTIFACTS = True

    # Threads per worker
    param_space["params"]["nthread"] = cpus_per_worker
    print("Per-trial CPUs =", num_workers * cpus_per_worker)

    # Scaling / placement
    scaling = ScalingConfig(
        num_workers=num_workers,
        use_gpu=False,
        resources_per_worker={"CPU": cpus_per_worker},
        trainer_resources={"CPU": 0},
        placement_strategy="PACK",
    )

    # Trainable
    trainer = XGBoostTrainer(
        label_column=label_col,
        params=param_space["params"],
        datasets={"train": train_ds, "valid": val_ds},
        num_boost_round=param_space["num_boost_round"],
        scaling_config=scaling,
    )

    # Search + scheduler
    MAX_T = int(param_space["num_boost_round"])
    GRACE = int(min(param_space.get("early_stopping_rounds", 1), MAX_T))
    algo = HyperOptSearch(metric="valid-rmse", mode="min")
    scheduler = ASHAScheduler(max_t=MAX_T, grace_period=GRACE, reduction_factor=3)

    # MLflow callback (child runs)
    mlflow_cb = MLflowLoggerCallback(
        tracking_uri=CLUSTER_TRACKING_URI,
        experiment_name=experiment_name,
        save_artifact=SAVE_ARTIFACTS,
        tags={"mlflow.parentRunId": parent_run_id},
    )

    # Tuner
    tuner = tune.Tuner(
        trainer.as_trainable(),
        run_config=RunConfig(
            name="xgb_from_s3_irsa",
            storage_path=TUNER_STORAGE,
            callbacks=[mlflow_cb],
        ),
        tune_config=tune.TuneConfig(
            search_alg=algo,
            scheduler=scheduler,
            metric="valid-rmse",
            mode="min",
            num_samples=NUM_SAMPLES,
            max_concurrent_trials=MAX_CONCURRENT,
        ),
        param_space={"params": param_space["params"]},
    )

    # Tune
    results = tuner.fit()
    best = results.get_best_result(metric="valid-rmse", mode="min")
    print("Best config:", best.config)
    print("Best valid RMSE:", best.metrics.get("valid-rmse"))

    # Final fit (train + val)
    merged = train_ds.union(val_ds)
    final_trainer = XGBoostTrainer(
        label_column=label_col,
        params=best.config["params"],
        datasets={"train": merged},
        num_boost_round=param_space["num_boost_round"],
        scaling_config=scaling,
        run_config=RunConfig(name="final_fit", storage_path=FINAL_STORAGE),
    )
    final_result = final_trainer.fit()
    final_ckpt = final_result.checkpoint

    # Load Booster from checkpoint
    with final_ckpt.as_directory() as ckpt_dir:
        print("Checkpoint dir:", ckpt_dir, "files:", os.listdir(ckpt_dir))
        candidates = ["model.json", "model.ubj", "model.xgb", "xgboost_model.json", "model"]
        model_path = next(
            (os.path.join(ckpt_dir, f) for f in candidates if os.path.exists(os.path.join(ckpt_dir, f))),
            None,
        )
        if not model_path:
            raise FileNotFoundError(f"No XGBoost model file found in checkpoint dir: {ckpt_dir}")
        booster = xgb.Booster()
        booster.load_model(model_path)

    # Driver-side eval (no Ray dependency)
    X_test = test_pdf.drop(columns=[label_col])
    
    dmat = xgb.DMatrix(X_test)
    y_pred = booster.predict(dmat)
    rmse = math.sqrt(((test_pdf[label_col].to_numpy() - y_pred) ** 2).mean())
    print(f"Test RMSE: {rmse:.4f}")

    
    # Log final under parent

    with mlflow.start_run(run_id=parent_run_id):
        X_example = X_test.head(5).copy()  
        y_example = booster.predict(xgb.DMatrix(X_example))
        sig = infer_signature(X_example, y_example)
        with mlflow.start_run(run_name="final_fit", nested=True):
            mlflow.log_params(best.config.get("params", {}))
            mlflow.log_dict({"label": label_col, "features": feature_cols}, "features.json")
            mlflow.log_metric("valid_rmse_best", float(best.metrics.get("valid-rmse")))
            mlflow.log_metric("test_rmse", float(rmse))
            mlflow_xgb.log_model(booster, artifact_path="model",signature=sig,input_example=X_example)
    run = client.get_run(parent_run_id)
    if run.info.status == "RUNNING":
        client.set_terminated(parent_run_id, "FINISHED")

@hydra.main(config_path=None, config_name="config", version_base=None)
def load_config_and_execute(cfg: DictConfig):
    print(f"Running in {cfg.env} environment")
    print(OmegaConf.to_yaml(cfg, resolve=True))
    app_name = cfg.app.name
    data_dir = cfg.app.data_dir
    experiment_name = cfg.mlflow.experiment_name    
    ray_workers = cfg.env.ray.num_workers
    cpus_per_worker = cfg.env.ray.cpus_per_worker
    dev_fast = cfg.env.ray.dev_fast    
    
    RAY_JOB_ENV = {
        "AWS_ROLE_ARN": os.environ.get("AWS_ROLE_ARN", ""),
        "AWS_WEB_IDENTITY_TOKEN_FILE": os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE", ""),
        "AWS_REGION": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")),
        "AWS_DEFAULT_REGION": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")),
        "TUNE_DISABLE_AUTO_CALLBACK_LOGGERS":"1",
        "TUNE_RESULT_BUFFER_LENGTH": "16",
        "TUNE_RESULT_BUFFER_FLUSH_INTERVAL_S": "3",    
        
    }
    ray.shutdown()
    ensure_ray_connected(RAY_JOB_ENV,ray_ns=app_name)    
    os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"] = "1"
    main(experiment_name=experiment_name,data_dir=data_dir, num_workers=4, cpus_per_worker=1,DEV_FAST=dev_fast)
    
if __name__ == "__main__":
    load_config_and_execute()