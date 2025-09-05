import os
import requests

def get_hw_tier_id(name: str):
    api_proxy = os.environ['DOMINO_API_PROXY']
    url = f"{api_proxy}/v4/hardwareTier"
    results = requests.get(url)
    hw_tiers = results.json()['hardwareTiers']
    for h in hw_tiers:
        if h['name']==name:
            return h

def get_environment_id(name: str):
    api_proxy = os.environ['DOMINO_API_PROXY']
    url = f"{api_proxy}/v4/environments/self"
    results = requests.get(url)
    envs = results.json()
    for e in envs:
        if e['name']==name:
            return e
