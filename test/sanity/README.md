# Sanity Test
There is a csi-sanity issue that it doesn't pass volume_context parameter to ControllerVolumeRequest() while SP has reported volume_context in CreateVolumeResponse().
I fix the code from kubernetes-csi/csi-test with v5.0.0 tag that released on Aug 4, 2022.

## Version
```
# csi-sanity -version
Version = (qsan)
```

## Usage
Generate a config.yaml file of StorageClass parameters for csi-sanity utility. Here is an example of config.yaml content:
```
protocol: iscsi
server: 192.168.217.201
pool: Pool_Johnson
iscsiPortals: 192.168.217.236
iscsiTargets: iqn.2004-08.com.qsan:xs3316-000ec9aed:dev3.ctr2
```

Run sanity test
```
# kubectl create -f testing.yaml
# csi-sanity --ginkgo.v -csi.endpoint dns:///${master_ip}:$(kubectl get "services/csi-service" -n qsan -o "jsonpath={..nodePort}") -csi.testvolumeparameters config.yaml
```
