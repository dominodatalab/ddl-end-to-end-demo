from typing import Dict
import ray
import os
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