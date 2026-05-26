package goiscsi

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"k8s.io/klog/v2"
)

func convertMapperPathToDM(mapperPath string) (string, error) {
	dmPath, err := os.Readlink(mapperPath)
	if err != nil {
		return "", fmt.Errorf("[ConvertMapperPathToDM] Readlink(%s) failed: %v", mapperPath, err)
	}

	parentDir := "/dev"
	baseName := filepath.Base(dmPath)
	return filepath.Join(parentDir, baseName), nil
}

func findDevicesByDM(dmPath string) ([]string, error) {
	// ex, get "dm-0" from "../dm-0", or "dm-0" from "/dev/dm-0"
	deviceName := filepath.Base(dmPath)

	sysfsPath := fmt.Sprintf("/sys/block/%s/slaves", deviceName)
	files, err := ioutil.ReadDir(sysfsPath)
	if err != nil {
		return nil, err
	}

	var devicePaths []string

	for _, file := range files {
		devicePath := fmt.Sprintf("/dev/%s", file.Name())
		devicePaths = append(devicePaths, devicePath)
	}

	if len(devicePaths) == 0 {
		return nil, fmt.Errorf("No slave devices not found for %s", dmPath)
	}

	return devicePaths, nil
}

func reloadMultipathDevice(devPath string) error {
	args := []string{"-r"}
	if devPath != "" {
		args = append(args, devPath)
	}
	if _, err := execCmd("multipath", args...); err != nil {
		return fmt.Errorf("Failed to reload multipath device %s: %v", devPath, err)
	}

	return nil
}

func resizeMultipathDevice(devPath string) error {
	args := []string{"resize", "map"}
	if _, err := execCmd("multipathd", append(args, devPath)...); err != nil {
		return fmt.Errorf("Failed to resize multipath device %s: %v", devPath, err)
	}

	return nil
}

func flushMultipathDevice(devPath string) error {
	if devPath != "" {
		args := []string{"-f"}
		if _, err := execCmd("multipath", append(args, devPath)...); err != nil {
			klog.Warningf("[FlushMultipathDevice] Flush %s device map failed after maximum retries: %v", devPath, err)
		} else {
			klog.V(2).Infof("[FlushMultipathDevice] Flush %s device map succeeded", devPath)
		}
	} else {
		if _, err := execCmd("multipath", []string{"-F"}...); err != nil {
			klog.Warningf("[FlushMultipathDevice] Flush all device maps failed after maximum retries: %v", err)
		} else {
			klog.V(2).Infof("[FlushMultipathDevice] Flush all device maps succeeded")
		}
	}

	return nil
}

func removeMultipathDevice(devPath string) error {
	args := []string{"-f"}
	err := retryOperation(func() error {
		_, err := execCmd("multipath", append(args, devPath)...)
		return err
	}, 5, time.Second*3)

	if err != nil {
		klog.Warningf("[RemoveMultipathDevice] Remove %s device failed after maximum retries: %v", devPath, err)
	} else {
		klog.V(2).Infof("[RemoveMultipathDevice] Remove %s device succeeded", devPath)

	}

	return nil
}

func retryOperation(operation func() error, maxRetries int, retryInterval time.Duration) error {
	var err error

	for retries := 0; retries < maxRetries; retries++ {
		err = operation()
		if err == nil {
			return nil
		}

		klog.Warningf("Operation failed: %v. Retrying in %s... (retries=%d)", err, retryInterval, retries)
		time.Sleep(retryInterval)
	}

	return errors.New("Exceeded maximum retries")
}
