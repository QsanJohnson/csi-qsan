## Usage
Method1: Use go test
```
# go test -timeout 3600s
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

Note: Remember to modify qsan-auth.yaml before testing.


## Test Scope
```
Namespace: qtest
StorageClass: qtest-xevo-storage
Scope:
  ReadWriteMany with rw/ro
  ReadWriteOnce with rw/ro
  ReadWriteMany block device  (using dd)
  ReadWriteOnce block device (using mke2fs)
  Online & offline volume expansion
  Web service with StatefulSet
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
+ Test expansion
  * Extend pvc-extend size to 21G
  * Extend pvc-rwm-file-rw size to 22G
  * Check if pvc-extend size is 21G
  * Check if pvc-rwm-file-rw filesystem size is 22G
+ TestWebService
  * Create nginx web server
  * Wait nginx pod ready
  * Generate index.html
  * Check web service
+ Cleanup
  * Delete nginx pod & service if exists
  * Delete nginx pvc if exists
  * Delete test pod if exists
  * Delete StorageClass if exists
  * Uninstall CSI Driver if deployed before
  
