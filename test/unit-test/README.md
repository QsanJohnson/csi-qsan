## Usage
Method1: Use go test
```
# go test -timeout 7200s
```

Method2: Use go run main utility
```
# go run test.go start
# go run test.go clean
```

Method3: Use go binary utility
```
# ./bin/test start
# ./bin/test clean
```

Note:
```
Remember to modify deploy/qsan-auth.yaml and test/unit-test/yaml/sc-m.yaml before testing.
If you want to test single path, change 'mpio' to 'false' in test.go and modify test/unit-test/yaml/sc.yaml before testing.
```


## Test Scope
```
Namespace: qtest
StorageClass: qtest-xevo-storage
Scope:
  Single path or MPIO (default MPIO)
  ReadWriteMany with rw/ro
  ReadWriteOnce with rw/ro
  ReadWriteMany block device  (using dd)
  ReadWriteOnce block device (using mke2fs)
  Online & offline volume expansion
  Web service with StatefulSet
  Clone
  Snapshot
Test elapsed time
```

## Test Flow
+ Deploy CSI Driver
  * Deploy CSI driver if not exists
+ Deploy StorageClass
+ Deploy test pod
+ Test Access
  * Verify ReadWriteMany with rw
  * Verify ReadWriteMany with ro
  * Verify ReadWriteOnce with rw
  * Verify ReadWriteOnce with ro
  * Verify ReadWriteMany block device using dd
  * Verify ReadWriteOnce block device using mke2fs
+ Test Expansion
  * Extend pvc-extend size to 21G
  * Extend pvc-rwm-file-rw size to 22G
  * Check if pvc-extend size is 21G
  * Check if pvc-rwm-file-rw filesystem size is 22G
+ Test WebService
  * Create nginx web server
  * Wait nginx pod ready
  * Generate index.html
  * Check web service
+ Test Clone
  * Run concurrent clone tests using multiple goroutines.
  * In each test, create 1 source PVC and pod, generate a test pattern, perform the clone operation, and compare the test patterns.
+ Deploy SnapshotClass
+ Test Snapshot
  * Verify the snapshot limit
  * Verify automatic snapshot cleanup mechanism
+ Test SnapshotRestore
  * Create 1 source PVC and pod, then execute a for loop test.
  * In each test iteration, generate a test pattern, take a snapshot, restore the snapshot, and compare the test pattern.
+ Cleanup
  * Delete nginx pod & service if exists
  * Delete nginx pvc if exists
  * Delete test pod if exists
  * Delete StorageClass if exists
  * Uninstall CSI Driver if deployed before
  
