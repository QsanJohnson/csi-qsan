#!/bin/sh
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

kubectl kustomize ${BASE_DIR}/ | kubectl delete -f -

if kubectl get volumesnapshotclass qsan-xevo-snapclass > /dev/null 2>&1; then
	kubectl delete -f "${BASE_DIR}/snapclass.yaml"
fi

if kubectl get node --show-labels | grep -q topology.qsan.com/fc; then
	kubectl label nodes --all topology.qsan.com/fc-
fi

if kubectl get node --show-labels | grep -q topology.qsan.com/model; then
	kubectl label nodes --all topology.qsan.com/model-
fi

if command -v oc >/dev/null 2>&1; then
    echo "Detected OpenShift, removing SCC bindings..."
    oc adm policy remove-scc-from-group privileged system:serviceaccounts:qsan || true
fi
