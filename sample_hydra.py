import hydra
from omegaconf import DictConfig

@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    print(f"Running in {cfg.env} environment")

if __name__ == "__main__":
    main()
