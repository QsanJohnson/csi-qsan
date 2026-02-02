#!/bin/sh
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

NAMESPACE="qsan"

if kubectl get node --show-labels | grep -q topology.qsan.com/fc; then
	kubectl label nodes --all topology.qsan.com/fc-
fi

if kubectl get node --show-labels | grep -q topology.qsan.com/model; then
	kubectl label nodes --all topology.qsan.com/model-
fi


if ! kubectl get crd volumesnapshots.snapshot.storage.k8s.io > /dev/null 2>&1; then
	kubectl kustomize ${BASE_DIR}/config/crd | kubectl create -f -
fi

if ! kubectl get deploy snapshot-controller -n kube-system > /dev/null 2>&1; then
	kubectl -n kube-system kustomize ${BASE_DIR}/snapshot-controller/ | kubectl create -f -
fi

kubectl kustomize ${BASE_DIR}/ | kubectl create -f -

if ! kubectl get volumesnapshotclass qsan-xevo-snapclass > /dev/null 2>&1; then
	kubectl apply -f "${BASE_DIR}/snapclass.yaml"
fi

if command -v oc >/dev/null 2>&1; then
    if oc get project >/dev/null 2>&1; then
        echo "Detected OpenShift environment, applying SCC..."
        oc adm policy add-scc-to-group privileged "system:serviceaccounts:${NAMESPACE}"
    fi
fi
