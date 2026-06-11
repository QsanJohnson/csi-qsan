
# goqsan
Go http client to manage Qsan XEVO models.

## Download
```
git clone http://gitlab.qsan.com/sharedlibs-go/goqsan
```

## Install
```
go get gitlab.qsan.com/sharedlibs-go/goqsan
```

## Usage
Here is an sample code.
```
import (
	"github.com/QsanJohnson/goqsan"
	"fmt"
	"context"
)
	
ctx := context.Background()

client := goqsan.NewClient("192.xxx.xxx.xxx")
systemAPI := goqsan.NewSystem(client)
res, err := systemAPI.GetAbout(ctx)
if err == nil {
	fmt.Printf("%+v\n", res);
}

authClient, err := client.GetAuthClient(ctx, "admin", "1234")
volumeAPI := goqsan.NewVolume(authClient)
vols, err := volumeAPI.ListVolumes(ctx, "")
if err == nil {
	fmt.Printf("%+v\n", vols);
}
```

## Debugging
Add flag.Parse() at the begining in main(),
then execute go run with "-v=4 -alsologtostderr" arguments.
```
go run test.go -v=4 -alsologtostderr
```


## Test Scope
It will depend on different models to perform different tests.
For example, 
* Q710 will have QoS tests, but QSM4 will not as the QoS function is not support at this time.
* QSM4 will have FileVolume, Share, Account and Sharehost tests, but Q710 will not.

|Tests         |Q710|QSM4| Comments                          |
|--------------|:--:|:--:|-----------------------------------|
|TestSystem    | V  | V  |                                   |
|TestNetwork   | V  | V  |                                   |
|TestHardware  | V  | V  |                                   |
|TestPool      | V  | V  |                                   |
|TestVolume    | V  | V  | includes Snapshot, Clone          |
|TestTarget    | V  | V  | includes iSCSI, FC                |
|TestAccount   |    | V  |                                   |
|TestFileVolume|    | V  | includes Share, Snapshot, Clone   |
|TestSharehost |    | V  | includes NFS, CIFS                |

Notes:
1. QSM4 series includes U710.
2. FC-related tests will be disabled in TestTarget if no FC card is present.
3. QSM4 doesn't support QoS function test in TestVolume.
4. Q710 doesn't support ClusterIP function test in TestNetwork.


## Testing

You have to create a test.conf file for integration test. Here are two examples:<br>
First example,
```
QSAN_IP = 192.xxx.xxx.xxx
QSAN_USERNAME = admin
QSAN_PASSWORD = 1234
POOL_ID = xxxxxx
```
* POOL_ID is Pool ID to be created/deleted volume on.

Second example (Recommend),
```
QSAN_IP = 192.xxx.xxx.xxx
QSAN_USERNAME = admin
QSAN_PASSWORD = 1234
POOL = xxxxxx
```
* POOL is Pool name to be created/deleted volume on.
* Will check if the pool exists before testing

<br>Then run integration test
```
go test -timeout 2h
```

Or run integration test with log level
```
export GOQSAN_LOG_LEVEL=4
go test -v -timeout 2h
```

Or run specified tests
```
// Only run TestVolume
go test -v -run=TestVolume

// Run both TestFileVolume and TestSharehost
go test -v -run="TestFileVolume|TestSharehost" -timeout 2h
```


