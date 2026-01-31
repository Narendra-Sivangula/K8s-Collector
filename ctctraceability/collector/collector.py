import os
import requests
from kubernetes import client, config
from datetime import datetime

print("COLLECTOR STARTED", flush=True)
OPENSEARCH = "http://opensearch.observability.svc.cluster.local:9200"

config.load_incluster_config()
v1 = client.CoreV1Api()
apps = client.AppsV1Api()

def push(index, data):
    r = requests.post(f"{OPENSEARCH}/{index}/_doc", json=data)
    print("OpenSearch response:", r.status_code, r.text, flush=True)

def deployment_mode():
    app = os.getenv("APP_NAME")
    ns = os.getenv("NAMESPACE", "default")

    dep = apps.read_namespaced_deployment(app, ns)
    selector = dep.spec.selector.match_labels
    label_selector = ",".join([f"{k}={v}" for k, v in selector.items()])
    pods = v1.list_namespaced_pod(ns, label_selector=label_selector)
    print(f"Deployment Mode: {app} in {ns}", flush=True)
    print("Using selector:", label_selector, flush=True)
    for p in pods.items:
        data = {
            "deployment": app,
            "namespace": ns,
            "pod": p.metadata.name,
            "image": p.spec.containers[0].image,
            "node": p.spec.node_name,
            "status": p.status.phase,
            "timestamp": datetime.utcnow().isoformat()
        }
        print("Sending data to OpenSearch", data, flush=True)
        push("deployment-metadata", data)

def cluster_mode():
    pods = v1.list_pod_for_all_namespaces()
    for p in pods.items:
        data = {
            "type": "pod",
            "namespace": p.metadata.namespace,
            "name": p.metadata.name,
            "status": p.status.phase,
            "node": p.spec.node_name,
            "timestamp": datetime.utcnow().isoformat()
        }
        push("cluster-metadata", data)

mode = os.getenv("MODE", "deployment")
if mode == "cluster":
    cluster_mode()
else:
    deployment_mode()
