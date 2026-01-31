import os
import requests
from kubernetes import client, config
from datetime import datetime

print("✅ COLLECTOR STARTED", flush=True)

# OpenSearch Endpoint
OPENSEARCH = "http://opensearch.observability.svc.cluster.local:9200"

# Load Kubernetes In-Cluster Config
config.load_incluster_config()

# Kubernetes Clients
v1 = client.CoreV1Api()
apps = client.AppsV1Api()


# ----------------------------------------------------------
# Push Document to OpenSearch
# ----------------------------------------------------------
def push(index, data):
    try:
        r = requests.post(f"{OPENSEARCH}/{index}/_doc", json=data)
        print("✅ OpenSearch Response:", r.status_code, r.text, flush=True)
    except Exception as e:
        print("❌ OpenSearch Push Failed:", str(e), flush=True)


# ----------------------------------------------------------
# Fetch Jenkins Metadata from OpenSearch (CI Traceability)
# ----------------------------------------------------------
def fetch_ci_metadata(image_digest):
    query = {
        "query": {
            "match": {
                "image_digest": image_digest
            }
        },
        "size": 1
    }

    try:
        r = requests.get(
            f"{OPENSEARCH}/ci-build-metadata/_search",
            json=query
        )

        if r.status_code != 200:
            print("❌ CI Lookup Failed:", r.text, flush=True)
            return None, None

        hits = r.json().get("hits", {}).get("hits", [])

        if not hits:
            print("⚠️ No CI Metadata Found For Tag:", image_tag, flush=True)
            return None, None

        source = hits[0]["_source"]

        build_id = source.get("build_id")

        commit_id = None
        commits = source.get("commits", [])
        if commits:
            commit_id = commits[0].get("commit_id")

        return build_id, commit_id

    except Exception as e:
        print("❌ Exception During CI Lookup:", str(e), flush=True)
        return None, None


# ----------------------------------------------------------
# Deployment Mode (ArgoCD PostSync Hook Mode)
# ----------------------------------------------------------
def deployment_mode():
    app = os.getenv("APP_NAME")
    ns = os.getenv("NAMESPACE", "default")

    print(f"\n✅ Deployment Mode Enabled", flush=True)
    print(f"🔹 App: {app}", flush=True)
    print(f"🔹 Namespace: {ns}", flush=True)

    # Read Deployment Object
    dep = apps.read_namespaced_deployment(app, ns)

    # Extract Deployment Info
    replicas = dep.spec.replicas
    strategy = dep.spec.strategy.type
    labels = dep.metadata.labels

    # Selector Labels for Pods
    selector = dep.spec.selector.match_labels
    label_selector = ",".join([f"{k}={v}" for k, v in selector.items()])

    print("✅ Using Pod Selector:", label_selector, flush=True)

    # Fetch Pods Belonging to Deployment
    pods = v1.list_namespaced_pod(ns, label_selector=label_selector)

    if not pods.items:
        print("⚠️ No Pods Found For Deployment!", flush=True)
        return

    # Loop Pods
    for p in pods.items:

        # Extract Image
        image = p.spec.containers[0].image
        image_tag = image.split(":")[-1]

        # Extract Digest
        digest = None
        if p.status.container_statuses:
            image_id = p.status.container_statuses[0].image_id
            if "sha256:" in image_id:
                digest = "sha256:" + image_id.split("sha256:")[-1]

        # Fetch Jenkins Metadata
        build_id, commit_id = fetch_ci_metadata(image_tag)

        # Final Traceability Document
        data = {

            # Deployment Identity
            "deployment": app,
            "namespace": ns,

            # Pod Identity
            "pod": p.metadata.name,

            # Runtime Info
            "image": image,
            "image_digest": digest,

            # Node + Status
            "node": p.spec.node_name,
            "status": p.status.phase,

            # Deployment Info
            "replicas": replicas,
            "strategy": strategy,
            "labels": labels,

            # CI/CD Traceability
            "build_id": build_id,
            "commit_id": commit_id,

            # Timestamp
            "timestamp": datetime.utcnow().isoformat()
        }

        print("\n🚀 Sending Deployment Trace Document:", flush=True)
        print(data, flush=True)

        # Push to OpenSearch
        push("deployment-metadata", data)


# ----------------------------------------------------------
# Cluster Mode (Optional Full Cluster Audit)
# ----------------------------------------------------------
def cluster_mode():
    print("\n✅ Cluster Mode Enabled", flush=True)

    pods = v1.list_pod_for_all_namespaces()

    for p in pods.items:
        data = {
            "type": "pod",
            "namespace": p.metadata.namespace,
            "pod": p.metadata.name,
            "status": p.status.phase,
            "node": p.spec.node_name,
            "timestamp": datetime.utcnow().isoformat()
        }

        push("cluster-metadata", data)


# ----------------------------------------------------------
# Main Execution
# ----------------------------------------------------------
mode = os.getenv("MODE", "deployment")

if mode == "cluster":
    cluster_mode()
else:
    deployment_mode()
