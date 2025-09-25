import os
import json
import math
import pandas as pd

import pyarrow as pa
import pyarrow.dataset as pds

from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split


import ray
from ray import tune
from ray.air import RunConfig, ScalingConfig

import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms

import ray
from ray import air, train
from ray.train import Checkpoint
from ray.train.torch import TorchTrainer, get_device, prepare_model, prepare_data_loader
from ray.air.config import RunConfig, ScalingConfig
from ray.air.config import RunConfig
from ray.tune.logger import CSVLoggerCallback, JsonLoggerCallback
from ray.runtime_context import get_runtime_context
from pathlib import Path

try:
    from ray.tune.callback import Callback      # Ray >= 2.6
except ImportError:
    from ray.tune.callbacks import Callback     # Older Ray
from utils import ray_utils

from pathlib import Path


import platform

def get_job_hex_safe(default="unknown_job"):
    try:
        if not ray.is_initialized():
            # connect or start a local Ray; if you only ever connect to a cluster, remove this
            ray.init(ignore_reinit_error=True)

        ctx = ray.get_runtime_context()

        # Newer Ray: ctx.job_id (may be an object with .hex or already a str)
        if hasattr(ctx, "job_id") and ctx.job_id is not None:
            jid = ctx.job_id
            return jid.hex() if hasattr(jid, "hex") else str(jid)

        # Older Ray: ctx.get_job_id()
        if hasattr(ctx, "get_job_id"):
            jid = ctx.get_job_id()
            return jid.hex() if hasattr(jid, "hex") else str(jid)
    except Exception:
        pass
    return default
    
def prepare_mnist(data_root: Path):
    data_root.mkdir(parents=True, exist_ok=True)
    # Uses torchvision's built-in downloader/extractor
    from torchvision import datasets, transforms
    tfm = transforms.Compose([transforms.ToTensor()])
    datasets.MNIST(str(data_root), train=True,  download=True, transform=tfm)
    datasets.MNIST(str(data_root), train=False, download=True, transform=tfm)






def build_model(num_classes: int = 10) -> nn.Module:
    return nn.Sequential(
        nn.Flatten(),
        nn.Linear(28 * 28, 512), nn.ReLU(),
        nn.Linear(512, 256), nn.ReLU(),
        nn.Linear(256, num_classes),
    )
    
def train_loop_per_worker(config):
    device = get_device()
    model = prepare_model(build_model().to(device))

    data_root = os.environ["SHARED_DIR"]  # already populated
    tfm = transforms.Compose([transforms.ToTensor()])

    # No network access in workers; just read the files
    train_ds = datasets.MNIST(data_root, train=True,  download=False, transform=tfm)
    test_ds  = datasets.MNIST(data_root, train=False, download=False, transform=tfm)

    # Start conservative; you can raise num_workers/pin_memory after it works
    train_loader = DataLoader(train_ds, batch_size=config["batch_size"], shuffle=True,
                              num_workers=0, pin_memory=False)
    test_loader  = DataLoader(test_ds,  batch_size=512, shuffle=False,
                              num_workers=0, pin_memory=False)

    train_loader = prepare_data_loader(train_loader)
    test_loader  = prepare_data_loader(test_loader)

    opt = torch.optim.AdamW(model.parameters(), lr=config["lr"])
    loss_fn = nn.CrossEntropyLoss()

    for epoch in range(config["epochs"]):
        model.train()
        running = 0.0
        for x, y in train_loader:
            x, y = x.to(device), y.to(device)
            opt.zero_grad(set_to_none=True)
            loss = loss_fn(model(x), y)
            loss.backward()
            opt.step()
            running += loss.item()

        model.eval()
        correct = total = 0
        with torch.no_grad():
            for x, y in test_loader:
                x, y = x.to(device), y.to(device)
                pred = model(x).argmax(dim=1)
                correct += (pred == y).sum().item()
                total += y.numel()
        acc = correct / total
        ##Demonstrates the train report
        train.report({"epoch": epoch, "train_loss": running, "val_acc": acc})



def main():
    data_dir = Path("/mnt/data/ddl-end-to-end-demo/")
    prepare_mnist(data_dir)
    RUNTIME_ENV = {
        "env_vars": {
        #"GLOO_SOCKET_IFNAME": "eth0",
        "SHARED_DIR": str(data_dir),
        "TUNE_DISABLE_AUTO_CALLBACKS": "1",
        "TORCH_DISABLE_ADDR2LINE": "1",     # stop symbolizer hang
        "TORCH_SHOW_CPP_STACKTRACES": "1",
        #"NCCL_IB_DISABLE": "1",
        #"NCCL_P2P_DISABLE": "1",
        #"NCCL_SHM_DISABLE": "1",
        "OMP_NUM_THREADS": "2",
        # >>> Key bits for DDP rendezvous <<<
        "MASTER_PORT": "29000",           # fixed, not ephemeral
        #"GLOO_SOCKET_IFNAME": "eth0",     # bind on pod interface
        #"NCCL_SOCKET_IFNAME": "eth0",     # harmless even if CPU-only
            
        }
    }
    # --------------------------------------
    if "RAY_HEAD_SERVICE_HOST" in os.environ and "RAY_HEAD_SERVICE_PORT" in os.environ:
       addr = f"ray://{os.environ['RAY_HEAD_SERVICE_HOST']}:{os.environ['RAY_HEAD_SERVICE_PORT']}"
       ray.shutdown()
       ray.init(
          address=addr or "auto",
          runtime_env=RUNTIME_ENV,   # same env you used earlier
          namespace="demo-ray-ns"
      )

    job_id_hex = get_job_hex_safe()

    DATASET_FOLDER = os.environ["DATASET_FOLDER"]
    shared = Path(DATASET_FOLDER,"ray_job",job_id_hex,"ray_results")  # pulled from runtime_env
    shared.mkdir(parents=True, exist_ok=True)
    STORAGE_PATH=str(shared)


    os.environ["TUNE_DISABLE_AUTO_CALLBACKS"] = "1"
    trainer = TorchTrainer(
        train_loop_per_worker,
        train_loop_config={"lr": 1e-3, "batch_size": 256, "epochs": 5},
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,                      # keep CPU+gloo until stable
            resources_per_worker={"CPU": 2,"GPU": 0},
            trainer_resources={"CPU": 0},               # <— key change
            placement_strategy="SPREAD",     
            #placement_strategy="PACK",          # single-node to avoid networking issues
        ),
        run_config=RunConfig(
            name=f"mnist_torch_ddp_{job_id_hex}",
            storage_path=STORAGE_PATH,
            callbacks=[CSVLoggerCallback(), JsonLoggerCallback()],
        ),
    )


    result = trainer.fit()

if __name__ == "__main__":
    
    main()