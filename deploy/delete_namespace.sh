#!/bin/sh

# This script mainly forces the deletion of the qsan namespace when its status is 'Terminating'.
STATUS=$(kubectl get namespace qsan -o jsonpath='{.status.phase}')

if [ -z "$STATUS" ]; then
	echo "'qsan' namespace is not exist"
elif [ "$STATUS" = "Active" ]; then
	echo "Ignore! 'qsan' namespace is Active."
elif [ "$STATUS" = "Terminating" ]; then
	kubectl get namespace qsan -o json | jq 'del(.spec.finalizers[])' | kubectl replace --raw /api/v1/namespaces/qsan/finalize -f -
	echo "Finalizer removed from namespace qsan"
else
	echo "'qsan' namespace is $STATUS"
fi

