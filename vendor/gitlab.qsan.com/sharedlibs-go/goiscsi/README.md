# goiscsi
A go package for iSCSI utility to manage iSCSI disk. <br>
It provides the following functions,
- Login
- GetDisk
- Logout

## Features
- Support CHAP
- Support MPIO
- Support timeout setting for iSCSI operation

## Design
### Login
Returns nil as long as one target is successfully logged in; otherwise return error. <br>
If target session already exists, bypass it and treat it as a successful login.

### Logout
Returns nil when all targets are successfully to logged out; otherwise return error. <br>
If target session does not exist, bypass it and treat it as a successful logout.

### GetDisk
GetDisk function will return Disk structure as below,
```
type Device struct {
	Name, Size            string
	Type, State           string
	Vendor, Model, Serial string
}

type Disk struct {
	Valid                 bool
	Status                string
	Name, Size            string
	Vendor, Model, Serial string
	MpathCnt, DiskCnt     int
	Devices               map[string]*Device
}
```
> Disk Valid: true if the data of Disk structure is valid, false otherwise <br>
> Disk Status: "online", "degrade", "offline", "mismatch" or "none"

The below describes several use cases for Valid and Status value.

Use case | Valid  | Status
---------|--------|-------
Normal   | true   | online
One device is offline or non-exist | true | degrade
All devices are offline | true | offline
Devices are not match | false | mismatch
No device exists | false | none


## Usage
Here is an sample code
```
import "test/goiscsi"

iscsi := &goiscsi.ISCSIUtil{Opts: goiscsi.ISCSIOptions{Timeout: 5000}}
tgts := []*goiscsi.Target{
    {Portal: "192.168.206.50:3260", Name: "iqn.2004-08.com.qsan:xf2026-000d42f58:dev3.ctr1", Lun: 0},
}

err := iscsi.Login(tgts)
if err != nil {
    fmt.Printf("Login failed: %v\n", err)
}

disk, err := iscsi.GetDisk(tgts)
fmt.Printf("Get disk: %+v\n", disk)
for name, dev := range disk.Devices {
    fmt.Printf("  %s: %+v\n", name, dev)
}

err = iscsi.Logout(tgts)
if err != nil {
    fmt.Printf("Logout failed: %v\n", err)
}
```

## Note
This package is designed for MPIO scenario that use device mapper multipathing.
Please set the value of find_multipaths in /etc/multipath.conf to 'no' to get better performance during getting scsi disk.


## Testing
You have to create a test.conf file for integration test. The following is a MPIO example with CHAP,
```
PORTALS = 192.168.206.50,192.168.206.51
NODES = iqn.2004-08.com.qsan:xf2026-000d42f58:dev3.ctr1,iqn.2004-08.com.qsan:xf2026-000d42f58:dev3.ctr2
LUNS = 0,0
CHAP_USER = johnson
CHAP_PASSWD = 111122223333
```
> Make sure the number of PORTALS, NODES and LUNS are the same for MPIO setting.

Then run integration test
```
go test -v
```

Or run integration test with log level
```
export GOISCSI_LOG_LEVEL=4
go test -v
```
