import requests
import os


base_url_of_cluster_scaler = "http://ddl-cluster-scaler-svc.domino-field.svc.cluster.local"
service_name="ddl_cluster_scaler"

def is_cluster_auto_scaler_healthy():
    # Is service running
    healthz_url = f"{base_url_of_cluster_scaler}/healthz"
    requests.get(healthz_url).text

