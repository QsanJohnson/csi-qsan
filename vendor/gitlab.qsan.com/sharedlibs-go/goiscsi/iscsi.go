// @2022 QSAN Inc. All right reserved

package goiscsi

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

type ISCSIUtil struct {
	Opts ISCSIOptions
}

type ISCSIOptions struct {
	Timeout   time.Duration // Millisecond
	ForceMPIO bool
}

type Chap struct {
	User, Passwd string
}

type Target struct {
	Portal string
	Name   string
	Lun    uint64
	Chap   *Chap
}

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

type Session struct {
	Portal      string
	Target      string
	State       string
	SCSIDevices []*SCSIDevice
}

type SCSIDevice struct {
	Lun   uint64
	Name  string
	State string
}

const (
	defaultPort        = "3260"
	deviceRetryCnt     = 10
	deviceRetryTimeout = 1000 // Millisecond
	dmRetryCnt         = 10
	dmRetryTimeout     = 2000 // Millisecond
)

func (iscsi *ISCSIUtil) Login(targets []*Target) error {
	success := false
	needRescan := false
	var err error
	sessions := getSessions()
	// var lun uint64 // Should only one even more targets
	for _, target := range targets {
		if targetSessionExists(sessions, target) {
			klog.V(4).Infof("Target session is already exist: %+v\n", target)
			// lun = target.Lun
			needRescan = true
			success = true
			continue
		}

		baseArgs := []string{"-m", "node", "-T", target.Name, "-p", target.Portal}
		if _, err = execCmd("iscsiadm", append(baseArgs, []string{"-o", "new"}...)...); err != nil {
			klog.Errorf("Failed to new node, err: %v", err)
		}

		if target.Chap != nil {
			if _, err = execCmd("iscsiadm", append(baseArgs, []string{"-o", "update",
				"-n", "node.session.auth.authmethod", "-v", "CHAP",
				"-n", "node.session.auth.username", "-v", target.Chap.User,
				"-n", "node.session.auth.password", "-v", target.Chap.Passwd}...)...); err != nil {

				klog.Errorf("Failed to set CHAP config, err: %v", err)
			}
		}

		ctx := context.Background()
		var cancel context.CancelFunc
		if iscsi.Opts.Timeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), iscsi.Opts.Timeout*time.Millisecond)
			defer cancel()
		}

		if _, err = execCmdContext(ctx, "iscsiadm", append(baseArgs, []string{"-l"}...)...); err != nil {
			klog.Errorf("Failed to login, err: %v", err)
		} else {
			success = true
		}
	}

	if needRescan {
		if err = rescanSession(nil); err != nil {
			klog.Errorf("rescanSession err: %v", err)
		}

		// iscsi.RescanDisk(strconv.FormatUint(lun, 10))
	}

	if success {
		return nil
	} else {
		return fmt.Errorf("Login failed, err: %v", err)
	}
}

func (iscsi *ISCSIUtil) Logout(targets []*Target) error {
	success := true
	var err error
	sessions := getSessions()
	for _, target := range targets {
		if !targetSessionExists(sessions, target) {
			klog.Warningf("Target session not exist: %+v\n", target)
			continue
		}

		ctx := context.Background()
		var cancel context.CancelFunc
		if iscsi.Opts.Timeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), iscsi.Opts.Timeout*time.Millisecond)
			defer cancel()
		}

		baseArgs := []string{"-m", "node", "-T", target.Name, "-p", target.Portal}
		if _, err = execCmdContext(ctx, "iscsiadm", append(baseArgs, []string{"-u"}...)...); err != nil {
			klog.Errorf("Failed to logout, err: %v", err)
		}

		if _, err = execCmd("iscsiadm", append(baseArgs, []string{"-o", "delete"}...)...); err != nil {
			klog.Errorf("Failed to delete node, err: %v", err)
			success = false
		}
	}

	if success {
		return nil
	} else {
		return fmt.Errorf("Logout failed, err: %v", err)
	}
}

func (iscsi *ISCSIUtil) GetSession() []*Session {
	return getSessions()
}

func (iscsi *ISCSIUtil) RescanAllSessions() error {
	return rescanSession(nil)
}

func (iscsi *ISCSIUtil) RescanSessionByTarget(targets []*Target) error {
	return rescanSession(targets)
}

func (iscsi *ISCSIUtil) RescanDisk(lun string) {
	scanLun := fmt.Sprintf("- - %s", lun)
	scsiHostPath := "/sys/class/scsi_host/"
	if hosts, err := ioutil.ReadDir(scsiHostPath); err == nil {
		for _, host := range hosts {
			procnamef := filepath.Join(scsiHostPath, host.Name(), "proc_name")
			data, err := os.ReadFile(procnamef)
			if err == nil && strings.HasPrefix(string(data), "iscsi") { // iscsi_tcp
				scanFile := filepath.Join(scsiHostPath, host.Name(), "scan")
				klog.V(4).Infof("[RescanDisk] echo \"%s\" > %s", scanLun, scanFile)
				if err := writeDeviceFile(scanFile, scanLun); err != nil {
					klog.Errorf("Failed to echo \"%s\" > %s, err: %v", scanLun, scanFile, err)
				}
			}
		}
	}

	if err := udevadmSettle(); err != nil {
		klog.Warningf("[RescanDisk] udevadmSettle failed: %v", err)
	}
}

func (iscsi *ISCSIUtil) GetDisk(targets []*Target) (*Disk, error) {
	sessions := getSessions()
	klog.V(2).Infof("[GetDisk] TargetCnt(%d) ForceMPIO(%v)", len(targets), iscsi.Opts.ForceMPIO)
	for _, t := range targets {
		klog.V(2).Infof("  %+v", t)
	}

	var devMap map[string]*Device
	var diskCnt, mpathCnt int
	// Wait dm device path ready
	for retries := 1; retries <= dmRetryCnt; retries++ {
		diskCnt, mpathCnt = 0, 0
		devMap, _ = getDevices(sessions, targets)
		for _, dev := range devMap {
			if dev.Type == "disk" || dev.Type == "mo-disk" {
				diskCnt++
			} else if dev.Type == "mpath" {
				mpathCnt++
			}
		}

		if iscsi.Opts.ForceMPIO {
			if mpathCnt == 0 && diskCnt > 0 {
				klog.Warningf("[GetDisk] MPIO, sleep %d msec then try again, retries=%d", dmRetryTimeout, retries)
				time.Sleep(time.Millisecond * dmRetryTimeout)
			} else {
				break
			}
		} else {
			if diskCnt == 0 {
				klog.Warningf("[GetDisk] No disk found")
			}
			break
		}
	}

	// Collect all device information to Disk structure
	var vendor, model, serial string
	var diskRunningNum int
	diskMatch := true
	disk := &Disk{}
	disk.DiskCnt = diskCnt
	disk.MpathCnt = mpathCnt
	disk.Devices = devMap
	for name, dev := range devMap {
		if dev.Type == "disk" || dev.Type == "mo-disk" {
			if vendor == "" {
				vendor, model, serial = dev.Vendor, dev.Model, dev.Serial
			} else {
				if vendor != dev.Vendor || model != dev.Model || serial != dev.Serial {
					diskMatch = false
				}
			}

			if dev.State == "running" {
				diskRunningNum++
			}
		} else if dev.Type == "mpath" {
			disk.Name = name
			disk.Size = dev.Size
		}
	}

	if diskMatch {
		disk.Vendor, disk.Model, disk.Serial = vendor, model, serial
	}

	if disk.MpathCnt == 1 && diskMatch {
		disk.Valid = true
	} else if !iscsi.Opts.ForceMPIO && disk.MpathCnt == 0 && disk.DiskCnt == 1 {
		disk.Valid = true
		// If no multipath, assign first device information with disk type to Disk structure
		for name, dev := range devMap {
			if dev.Type == "disk" || dev.Type == "mo-disk" {
				disk.Name = name
				disk.Size = dev.Size
				break
			}
		}
	}

	switch {
	case disk.DiskCnt == 0:
		disk.Status = "none"
	case diskMatch == false:
		disk.Status = "mismatch"
	case disk.Valid && diskRunningNum == len(targets):
		disk.Status = "online"
	case disk.Valid && diskRunningNum == 0:
		disk.Status = "offline"
	case disk.Valid && diskRunningNum < len(targets):
		disk.Status = "degrade"
	default:
		disk.Status = "unknown"
	}

	if iscsi.Opts.ForceMPIO && disk.MpathCnt == 0 && disk.DiskCnt > 0 {
		// If the device's dm device is not created, reload multipath device for the next GetDisk() function call.
		// For example,
		// # lsblk /dev/sdv
		// NAME                MAJ:MIN RM SIZE RO TYPE  MOUNTPOINT
		// sdv                  65:80   0   5G  0 disk
		klog.Warningf("[GetDisk] Disk multipath device not exist, reload multipath device. %+v", disk)
		if err := reloadMultipathDevice(""); err != nil {
			klog.Warningf("[GetDisk] Reload multipath device failed. %v", err)
		}
	}

	if iscsi.Opts.ForceMPIO && disk.MpathCnt > 1 {
		// Should not be run here. This is a patch code.
		klog.Warningf("[GetDisk] There are %d multipath devices present. %v", disk.MpathCnt, disk)

		for name, dev := range devMap {
			if dev.Type == "mpath" {
				flushMultipathDevice("/dev/" + name)
			}
		}
	}

	return disk, nil
}

func (iscsi *ISCSIUtil) RemoveDisk(diskPath string) error {
	var devicePaths []string
	var err error
	var needFlushMp bool

	dmPath := diskPath
	if strings.HasPrefix(diskPath, "/dev/mapper/") {
		if dmPath, err = convertMapperPathToDM(diskPath); err != nil {
			return err
		}
	}
	if strings.HasPrefix(dmPath, "/dev/dm-") {
		if devicePaths, err = findDevicesByDM(dmPath); err != nil {
			return err
		}
		klog.Infof("[RemoveDisk] Remove multipath device %s (%s)", dmPath, diskPath)
		if err = removeMultipathDevice(diskPath); err != nil {
			return err
		}
	} else {
		devicePaths = append(devicePaths, diskPath)
	}

	klog.Infof("[RemoveDisk] Remove diskPath(%s): %v", diskPath, devicePaths)
	for _, devPath := range devicePaths {
		if strings.HasPrefix(devPath, "/dev/") {
			var tgts []*Target
			target, err := getIscsiTargetFromDevice(devPath)
			if err != nil {
				return fmt.Errorf("getIscsiTargetFromDevice failed: %v", err)
				return nil
			}
			klog.V(4).Infof("[RemoveDisk] target: %+v\n", target)
			tgts = append(tgts, target)

			if err = removeDevice(devPath); err != nil {
				klog.Errorf("[RemoveDisk] Failed to remove device %s: %v", devPath, err)
			}

			hasUsedDisk, _ := iscsi.HasAnotherUsedDisk(tgts)
			klog.V(4).Infof("[RemoveDisk] hasUsedDisk %v", hasUsedDisk)
			if !hasUsedDisk {
				err = iscsi.Logout(tgts)
				if err != nil {
					klog.Warningf("[RemoveDisk] iSCSI logout failed, err: %v", err)
				}
				for _, t := range tgts {
					klog.Infof("[RemoveDisk] iSCSI logout %v", t)
				}
				needFlushMp = true
			} else {
				klog.V(4).Infof("[RemoveDisk] Bypass iSCSI logout")
			}
		} else {
			klog.Errorf("[RemoveDisk] Invalid device path: %s", devPath)
		}
	}

	if strings.HasPrefix(diskPath, "/dev/mapper/") && needFlushMp {
		flushMultipathDevice("")
	}

	return nil
}

func (iscsi *ISCSIUtil) ExpandDisk(diskPath, lun string) error {
	iscsi.RescanAllSessions()
	// iscsi.RescanDisk(lun)

	dmPath := diskPath
	var err error
	if strings.HasPrefix(diskPath, "/dev/mapper/") {
		if dmPath, err = convertMapperPathToDM(diskPath); err != nil {
			return err
		}
	}
	if len(dmPath) > 0 {
		klog.Infof("[ExpandDisk] Resize multipath device %s (%s)", dmPath, diskPath)
		if err := resizeMultipathDevice(dmPath); err != nil {
			return err
		}

		syscall.Sync()
	}

	return nil
}

func (iscsi *ISCSIUtil) IsSessionExist(targets []*Target) bool {
	sessions := getSessions()
	for _, target := range targets {
		if targetSessionExists(sessions, target) {
			return true
		}
	}

	return false
}

func (iscsi *ISCSIUtil) HasAnotherUsedDisk(targets []*Target) (bool, error) {
	return hasMntDevices(targets)
}
