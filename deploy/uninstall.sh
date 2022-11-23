#!/bin/sh
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

kubectl delete -f ${BASE_DIR}/node.yaml
kubectl delete -f ${BASE_DIR}/controller.yaml
kubectl delete -f ${BASE_DIR}/driverinfo.yaml
kubectl delete -f ${BASE_DIR}/rbac.yaml

kubectl delete secret qsan-auth-secret -n qsan
kubectl delete ns qsan
