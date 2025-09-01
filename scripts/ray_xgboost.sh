pwd
ls -la /mnt/code/conf
ls -la /mnt/code/conf/env

export HYDRA_FULL_ERROR=1
python -m ray_examples.xgboost --config-dir /mnt/code/conf env=dev