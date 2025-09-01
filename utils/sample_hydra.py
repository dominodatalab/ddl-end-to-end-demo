import hydra
from omegaconf import DictConfig
from omegaconf import OmegaConf

@hydra.main(config_path=None, config_name="config", version_base=None)
def main(cfg: DictConfig):
    print(f"Running in {cfg.env} environment")
    print(OmegaConf.to_yaml(cfg, resolve=True))
    
if __name__ == "__main__":
    main()