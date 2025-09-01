import requests
import os
import time
import json

base_url_of_cluster_scaler = "http://ddl-cluster-scaler-svc.domino-field.svc.cluster.local"
service_name="ddl_cluster_scaler"

def is_cluster_auto_scaler_healthy():
    # Is service running
    healthz_url = f"{base_url_of_cluster_scaler}/healthz"
    result = requests.get(healthz_url)
    return result.status_code, result.text

def get_auth_headers():
    domino_api_proxy= os.environ['DOMINO_API_PROXY']
    #This is a proxy for the service account tokem
    token = requests.get(f"{domino_api_proxy}/access-token").text
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"   # add this if you're posting JSON
    }
    return headers 

def get_cluster_status(cluster_kind:str="rayclusters"):  
    cluster_id_prefix = cluster_kind.replace("clusters","")
    cluster_id = f"{cluster_id_prefix}-{os.environ['DOMINO_RUN_ID']}"
    print(cluster_id)
    get_cluster_url = f"{base_url_of_cluster_scaler}/{service_name}/get/{cluster_kind}/{cluster_id}"
    print(get_cluster_url)
    resp = requests.get(get_cluster_url,headers=get_auth_headers())
    print(f"Status code {resp.status_code}")
    cluster_details = resp.json()
    return cluster_details


def scale_cluster(cluster_kind:str="rayclusters",replicas:int=1):
    cluster_id_prefix = cluster_kind.replace("clusters","")
    cluster_id = f"{cluster_id_prefix}-{os.environ['DOMINO_RUN_ID']}"
    scale_cluster_url = f"{base_url_of_cluster_scaler}/{service_name}/scale/{cluster_kind}/{cluster_id}"
    print(scale_cluster_url)
    
    resp = requests.post(
        scale_cluster_url,
        headers=get_auth_headers(),
        json={"replicas": replicas},
        timeout=(3.05, 10),
    )
    print(f"Status code {resp.status_code}")
    cluster_details = resp.json()
    return cluster_details

def is_scaling_complete(cluster_kind:str="rayclusters") -> bool:
    result = get_cluster_status()
    #print(json.dumps(result, indent=2))
    effective_replicas = result['spec']['autoscaling']['minReplicas']
    nodes = result['status']['nodes']
    is_complete = (len(nodes)==(effective_replicas+1))

    print(f"Expected worker nodes {effective_replicas}")    
    print(f"Current worker nodes {nodes[1:]}")
    return is_complete

def wait_until_scaling_complete(cluster_kind:str="rayclusters") -> bool:
    is_complete = is_scaling_complete(cluster_kind)
    while not is_complete:
        print("Scaling not yet done...")
        time.sleep(2)
        is_complete = is_scaling_complete(cluster_kind)
    return is_complete