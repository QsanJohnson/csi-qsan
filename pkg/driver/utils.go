package driver

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/mount"

	"k8s.io/klog/v2"
)

func NewDefaultIdentityServer(d *Driver) *IdentityServer {
	return &IdentityServer{
		Driver: d,
	}
}

func NewControllerServer(d *Driver) *ControllerServer {
	return &ControllerServer{
		Driver: d,
	}
}

func NewControllerServiceCapability(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
	return &csi.ControllerServiceCapability{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{
				Type: cap,
			},
		},
	}
}

func NewNodeServiceCapability(cap csi.NodeServiceCapability_RPC_Type) *csi.NodeServiceCapability {
	return &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: cap,
			},
		},
	}
}

func ParseEndpoint(ep string) (string, string, error) {
	if strings.HasPrefix(strings.ToLower(ep), "unix://") || strings.HasPrefix(strings.ToLower(ep), "tcp://") {
		s := strings.SplitN(ep, "://", 2)
		if s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("Invalid endpoint: %v", ep)
}

func hideLog(method string) bool {
	if method == "/csi.v1.Identity/Probe" ||
		method == "/csi.v1.Node/NodeGetCapabilities" {
		return true
	}
	return false
}

func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if !hideLog(info.FullMethod) {
		klog.Infof("GRPC call: %s", info.FullMethod)
		klog.Infof("GRPC request: %s", protosanitizer.StripSecrets(req))
	}

	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf("GRPC error: %v", err)
	} else {
		if !hideLog(info.FullMethod) {
			klog.Infof("GRPC response: %s", protosanitizer.StripSecrets(resp))
		}
	}
	return resp, err
}

type VolumeLocks struct {
	locks sets.String
	mux   sync.Mutex
}

func NewVolumeLocks() *VolumeLocks {
	return &VolumeLocks{
		locks: sets.NewString(),
	}
}

func (vl *VolumeLocks) TryAcquire(volumeID string) bool {
	vl.mux.Lock()
	defer vl.mux.Unlock()
	if vl.locks.Has(volumeID) {
		return false
	}
	vl.locks.Insert(volumeID)
	return true
}

func (vl *VolumeLocks) Release(volumeID string) {
	vl.mux.Lock()
	defer vl.mux.Unlock()
	vl.locks.Delete(volumeID)
}

func execCmd(name string, args ...string) (string, error) {
	// klog.Infof("[execCmd] %s, args=%+v \n", name, args)
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	// klog.Infof("[execCmd] Output ==>\n%+v\n", string(out))
	if err != nil {
		return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
	}

	return string(out), err
}

func ipv4ToHexStr(ipv4 string) string {
	r := `^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})`
	reg, err := regexp.Compile(r)
	if err != nil {
		return ""
	}
	ips := reg.FindStringSubmatch(ipv4)
	if ips == nil {
		return ""
	}
	ip1, _ := strconv.Atoi(ips[1])
	ip2, _ := strconv.Atoi(ips[2])
	ip3, _ := strconv.Atoi(ips[3])
	ip4, _ := strconv.Atoi(ips[4])
	buf := []byte{byte(ip1), byte(ip2), byte(ip3), byte(ip4)}
	return hex.EncodeToString(buf)
}

func hexStrToIPv4(s string) string {
	b, err := hex.DecodeString(s)
	if err != nil {
		fmt.Println("HexStrToIPv4 failed:", s)
	}
	return fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
}

func ipv4ToByte(ipv4 string) []byte {
	r := `^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})`
	reg, err := regexp.Compile(r)
	if err != nil {
		return nil
	}
	ips := reg.FindStringSubmatch(ipv4)
	if ips == nil {
		return nil
	}
	ip1, _ := strconv.Atoi(ips[1])
	ip2, _ := strconv.Atoi(ips[2])
	ip3, _ := strconv.Atoi(ips[3])
	ip4, _ := strconv.Atoi(ips[4])
	return []byte{byte(ip1), byte(ip2), byte(ip3), byte(ip4)}
}

func IsCorruptedDir(dir string) bool {
	_, pathErr := mount.PathExists(dir)
	return pathErr != nil && mount.IsCorruptedMnt(pathErr)
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	defer f.Close()
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func splitAndTrimSpace(s, sep string) []string {
	result := strings.Split(s, sep)

	for i := 0; i < len(result); i++ {
		result[i] = strings.TrimSpace(result[i])
	}

	return result
}

func retryOperation(operation func() error, maxRetries int, retryInterval time.Duration) error {
	var err error

	for retries := 0; retries < maxRetries; retries++ {
		err = operation()
		if err == nil {
			return nil
		}

		klog.Warningf("Operation failed: %v. Retrying in %s...\n", err, retryInterval)
		time.Sleep(retryInterval)
	}

	return errors.New("Exceeded maximum retries")
}

func syncfs(fsPath string) error {
	dir, err := os.Open(fsPath)
	if err != nil {
		return err
	}
	defer dir.Close()

	err = syscall.Fsync(int(dir.Fd()))
	if err != nil {
		return err
	}

	return nil
}

func showMountFsSize(mountPoint string) {
	cmd := exec.Command("df", mountPoint)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Warningf("[showMountFsSize] Error running df: %v", err)
		return
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		klog.Warningf("[showMountFsSize] df output does not contain expected lines.")
		return
	}

	// The below is an output example,
	// Filesystem                    1K-blocks  Used Available Use% Mounted on
	// /dev/mapper/32bb30013781126c0  10218772    24  10202364   1% /var/lib/kubelet/plugins/kubernetes.io/csi/xevo.csi.qsan.com/3e1ff781457f3ccef786eb619218e8ece0ccc7e89326f0744a910e44828f653a/globalmount
	fields := strings.Fields(lines[1])
	if len(fields) < 4 {
		klog.Warningf("[showMountFsSize] Unable to parse df output.")
		return
	}
	deviceName := fields[0]
	totalSize := fields[1]

	args := []string{"-rn", "-o", "SIZE"}
	sizeStr, err := execCmd("lsblk", append(args, []string{deviceName}...)...)
	if err != nil {
		klog.Warningf("[showMountFsSize] Failed to get DeviceName(%s) size", deviceName)
		return
	}
	sizeStr = strings.TrimSpace(string(sizeStr))

	klog.Infof("[showMountFsSize] DeviceName(%s) DeviceSize(%s) MountFsSize(%s 1K-blocks)", deviceName, sizeStr, totalSize)
}

func hasRDMADevice() bool {
	const infinibandDir = "/sys/class/infiniband/"
	entries, err := ioutil.ReadDir(infinibandDir)
	if err != nil {
		klog.Infof("[hasRDMADevice] Cannot read %s: %v", infinibandDir, err)
		return false
	}
	return len(entries) > 0
}

func getCallerFunctionName() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return "unknown"
	}
	fullName := runtime.FuncForPC(pc).Name()
	parts := strings.Split(fullName, "/")
	if len(parts) == 0 {
		return fullName
	}
	return parts[len(parts)-1]
}
