// @2022 QSAN Inc. All right reserved

package goiscsi

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
	kexec "k8s.io/utils/exec"
	mount "k8s.io/utils/mount"
)

func getSessions() []*Session {
	var sessions []*Session

	args := []string{"-m", "session", "-P", "3"}
	out, err := execCmd("iscsiadm", args...)
	if err != nil {
		klog.V(2).Infof("getSessions: %v", err)
		return sessions
	}

	var curTarget string
	var curSession *Session
	var scsiDev *SCSIDevice
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		switch {
		case strings.HasPrefix(line, "Target:"):
			curTarget = strings.Fields(line)[1]
		case strings.HasPrefix(line, "Current Portal:"):
			tmpSession := Session{
				Target: curTarget,
				Portal: strings.Split(sessionFieldValue(line), ",")[0],
			}
			curSession = &tmpSession
			sessions = append(sessions, curSession)
		case strings.HasPrefix(line, "iSCSI Session State:"):
			curSession.State = sessionFieldValue(line)
		case strings.HasPrefix(line, "scsi"):
			lun, _ := strconv.ParseUint(sessionFieldValue(line), 10, 32)
			tmpScsiDev := SCSIDevice{Lun: lun}
			scsiDev = &tmpScsiDev
			curSession.SCSIDevices = append(curSession.SCSIDevices, scsiDev)
		case strings.HasPrefix(line, "Attached scsi disk"):
			scsiDev.Name = strings.Fields(line)[3]
			scsiDev.State = sessionFieldValue(line)
		}
	}

	return sessions
}

func rescanSession(targets []*Target) error {
	if targets == nil {
		args := []string{"-m", "session", "--rescan"}
		if _, err := execCmd("iscsiadm", args...); err != nil {
			return fmt.Errorf("Failed to rescan session, err: %v", err)
		}
	} else {
		for _, target := range targets {
			args := []string{"-m", "node", "-T", target.Name, "--rescan"}
			if _, err := execCmd("iscsiadm", args...); err != nil {
				return fmt.Errorf("Failed to rescan session of target(%s), err: %v", target.Name, err)
			}
		}
	}

	return nil
}

func getDevices(sessions []*Session, targets []*Target) (map[string]*Device, error) {
	var devs []*Device
	devMap := make(map[string]*Device)
	for _, target := range targets {
		devicePath := strings.Join([]string{"/dev/disk/by-path/ip", target.Portal, "iscsi", target.Name, "lun", fmt.Sprint(target.Lun)}, "-")
		klog.V(4).Infof("[getDevices] devicePath=%s", devicePath)

		// Wait device path ready if device lun session exists
		exists := false
		for retries := 1; retries <= deviceRetryCnt; retries++ {
			_, err := os.Stat(devicePath)
			if os.IsNotExist(err) && lunSessionExists(sessions, target) {
				klog.V(3).Infof("[getDevices] sleep %d msec then try again, retries=%d (%s)", deviceRetryTimeout, retries, devicePath)
				time.Sleep(time.Millisecond * deviceRetryTimeout)
			} else {
				exists = true
				break
			}
		}

		if exists {
			linkDevice, err := filepath.EvalSymlinks(devicePath)
			if err != nil {
				klog.Warningf("Failed to get link device of %s: %v", devicePath, err)
			}
			klog.V(2).Infof("[getDevices] devicePath(%s) linkDevice(%s)", devicePath, linkDevice)

			args := []string{"-rn", "-o", "NAME,KNAME,PKNAME,TYPE,STATE,SIZE,VENDOR,MODEL,WWN"}
			out, err := execCmd("lsblk", append(args, devicePath)...)
			if err == nil {
				lines := strings.Split(strings.Trim(out, "\n"), "\n")
				for _, line := range lines {
					tokens := strings.Split(line, " ")
					klog.V(2).Infof("[getDevices] deviceInfo %+v\n", tokens)
					dev := &Device{
						Name:   tokens[0],
						Type:   tokens[3],
						State:  tokens[4],
						Size:   tokens[5],
						Vendor: tokens[6],
						Model:  tokens[7],
						Serial: tokens[8],
					}
					devs = append(devs, dev)
					devMap[tokens[1]] = dev
				}
			} else {
				fmt.Printf("Failed to get disk path : %v \n", err)
			}
		}
	}

	return devMap, nil
}

func hasMntDevices(targets []*Target) (bool, error) {
	cnt, total := 0, 0
	prefixDir := "/dev/disk/by-path/"

	var devPaths []string
	for _, target := range targets {
		devPrefixName := strings.Join([]string{"ip", target.Portal, "iscsi", target.Name, "lun"}, "-")

		files, err := ioutil.ReadDir(prefixDir)
		if err != nil {
			return false, fmt.Errorf("Failed to ReadDir: %v", err)
		}

		for _, file := range files {
			if strings.HasPrefix(file.Name(), devPrefixName) {
				total++

				args := []string{"-rn", "-o", "NAME,KNAME,MOUNTPOINT"}
				devicePath := filepath.Join(prefixDir, file.Name())
				out, err := execCmd("lsblk", append(args, devicePath)...)
				if err == nil {
					lines := strings.Split(strings.Trim(out, "\n"), "\n")
					for _, line := range lines {
						tokens := strings.Split(line, " ")
						devPaths = append(devPaths, tokens[1])
						mntPath := tokens[2]
						if len(mntPath) > 0 {
							klog.V(2).Infof("[hasMntDevices] %s, mountpoint(%s)", file.Name(), mntPath)
							cnt++
							// Directly return true to optimize the performance of hasMntDevices function.
							// break
							return true, nil
						}
					}
				}
			}
		}
	}

	klog.V(2).Infof("[hasMntDevices] cnt: %d/%d, devPaths: %+v", cnt, total, devPaths)

	mounter := &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      kexec.New(),
	}
	mnts, err := mounter.List()
	if err != nil {
		klog.V(2).Infof("[hasMntDevices] List mount err: %v\n", err)
	}
	for _, mp := range mnts {
		var devName string
		if mp.Device == "udev" {
			if mp.Path == "/dev" || mp.Path == "/host/dev" {
				// not a block device
				continue
			}
			klog.V(5).Infof("[hasMntDevices] udev_mp %+v", mp)
			args := []string{"-rn", "-o", "KNAME"}
			out, err := execCmd("lsblk", append(args, mp.Path)...)
			if err == nil {
				devName = strings.Trim(string(out), "\n")
			}

		} else if strings.HasPrefix(mp.Device, "/dev/mapper/") {
			klog.V(5).Infof("[hasMntDevices] mpath_mp %+v", mp)
			args := []string{"-rn", "-o", "KNAME"}
			out, err := execCmd("lsblk", append(args, mp.Device)...)
			if err == nil {
				devName = strings.Trim(string(out), "\n")
			}

		} else if strings.HasPrefix(mp.Device, "/dev/") {
			klog.V(5).Infof("[hasMntDevices] dev_mp %+v", mp)
			devName = mp.Device[5:]
		} else {
			continue
		}

		if len(devName) > 0 && contains(devPaths, devName) {
			klog.V(2).Infof("[hasMntDevices] Found %s path(%s) devName(%s)", mp.Device, mp.Path, devName)
			return true, nil
		}
	}

	return cnt > 0, nil
}

func lunSessionExists(sessions []*Session, target *Target) bool {
	for _, sess := range sessions {
		if sess.Portal == target.Portal && sess.Target == target.Name {
			for _, scsiDev := range sess.SCSIDevices {
				if scsiDev.Lun == target.Lun {
					return true
				}
			}
		}
	}

	return false
}

func targetSessionExists(sessions []*Session, target *Target) bool {
	for _, sess := range sessions {
		if sess.Portal == target.Portal && sess.Target == target.Name {
			return true
		}
	}

	return false
}

func getIscsiTargetFromDevice(dev string) (*Target, error) {
	path, err := getIscsiDevicePath(dev)
	if err != nil {
		return nil, err
	}

	portal, iqn, lun, err := parseIscsiDevicePath(path)
	if err != nil {
		return nil, err
	}

	lunNum, _ := strconv.ParseUint(lun, 10, 32)
	target := &Target{
		Portal: portal,
		Name:   iqn,
		Lun:    lunNum,
	}

	return target, nil
}

func getIscsiDevicePath(dev string) (string, error) {
	prefixDir := "/dev/disk/by-path/"
	files, err := ioutil.ReadDir(prefixDir)
	if err != nil {
		return "", fmt.Errorf("Failed to ReadDir: %v", err)
	}

	for _, file := range files {
		// fmt.Printf("File Name: %s \n", file.Name())

		if strings.HasPrefix(file.Name(), "ip-") {
			fi, err := os.Lstat(filepath.Join(prefixDir, file.Name()))
			if err != nil {
				return "", fmt.Errorf("getIscsiDevicePath(%s), Lstat %s failed: %v", dev, filepath.Join(prefixDir, file.Name()), err)
			}
			if fi.Mode()&fs.ModeSymlink != 0 {
				link, _ := os.Readlink(filepath.Join(prefixDir, file.Name()))
				// fmt.Printf("link: %s, err=%v \n", link, err)

				realDev, _ := filepath.Abs(prefixDir + link)
				// fmt.Printf("real: %s\n", realDev)
				if realDev == dev {
					// portal, iqn, lun, err := ParseIscsiDevicePath(file.Name())
					// if err != nil {
					// 	fmt.Printf("ParseiSCSIDevicePath err: %v\n", err)
					// }
					// return portal, iqn, lun, nil
					klog.Infof("[getIscsiDevicePath] Found %s ==> %s", dev, file.Name())
					return file.Name(), nil
				}
			}
		}
	}

	return "", fmt.Errorf("No iSCSI device path found. (%s)", dev)
}

func parseIscsiDevicePath(s string) (portal, iqn, lun string, err error) {
	tokens := strings.Split(s, "-")
	// The valid format example, "ip-192.168.217.236:3260-iscsi-iqn.2004-08.com.qsan:xs3316-000ec9aed:dev5.ctr2-lun-0"
	if tokens[0] == "ip" && tokens[2] == "iscsi" && tokens[len(tokens)-2] == "lun" {
		portal = tokens[1]
		lun = tokens[len(tokens)-1]

		var iqn_tok []string
		for i := 3; i < len(tokens)-2; i++ {
			iqn_tok = append(iqn_tok, tokens[i])
		}
		iqn = strings.Join(iqn_tok, "-")

		return portal, iqn, lun, nil

	} else {
		return "", "", "", fmt.Errorf("invalid device format: %s", s)
	}
}

func removeDevice(devPath string) error {
	if strings.HasPrefix(devPath, "/dev/") {
		args := []string{"--flushbufs"}
		if _, err := execCmd("blockdev", append(args, []string{devPath}...)...); err != nil {
			klog.Errorf("Failed to flush buffer to %s: %v", devPath, err)
		}

		devName := devPath[5:]
		devFile := fmt.Sprintf("/sys/block/%s/device/state", devName)
		if err := writeDeviceFile(devFile, "offline\n"); err != nil {
			return fmt.Errorf("failed to echo offline > %s: %v\n", devFile, err)
		}

		devFile = fmt.Sprintf("/sys/block/%s/device/delete", devName)
		if err := writeDeviceFile(devFile, "1"); err != nil {
			return fmt.Errorf("failed to echo 1 > %s: %v\n", devFile, err)
		}
	} else {
		return fmt.Errorf("invalid device path: %s", devPath)
	}

	return nil
}

// Reference the udevadm_settle() in rescan-scsi-bus.sh
func udevadmSettle() error {
	tmo := 60

	for tmo > 0 {
		out, err := execCmd("udevadm", []string{"settle", "--timeout=1"}...)
		if err != nil {
			klog.V(2).Infof("failed to execute udevadm settle: %v", err)
			time.Sleep(time.Second)
			break
		}

		if !hasSdDevices(out) {
			time.Sleep(time.Second)
			break
		}
		klog.V(2).Infof("[udevadmSettle] output: %s (tmo=%d)", out, tmo)

		tmo--
		time.Sleep(time.Second)
	}

	return nil
}

func hasSdDevices(output string) bool {
	regex := regexp.MustCompile(`sd[a-z]+`)
	return regex.MatchString(output)
}
