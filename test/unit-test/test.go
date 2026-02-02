package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	mpio = true

	numTests        = 1
	numCloneTests   = 3
	numRestoreTests = 3
)

var csi_exist bool
var protocol string
var err error

var tests = []struct {
	name    string
	fnExec  func() error
	fnClean func() error
}{
	{"DeployCSIDriver", DeployCSIDriver, DeployCSIDriver_Cleanup},
	{"DeployStorageClass", DeployStorageClass, DeployStorageClass_Cleanup},
	{"DeployPVC", CreatePVC, CreatePVC_Cleanup},
	{"DeployPod", DeployPod, DeployPod_Cleanup},
	{"TestAccess", TestAccess, nil},
	{"TestExpansion", TestExpansion, nil},
	{"TestWebService", TestWebService, TestWebService_Cleanup},
	{"TestClone", TestClone, TestClone_Cleanup},
	{"DeploySnapshotClass", DeploySnapshotClass, DeploySnapshotClass_Cleanup},
	{"TestSnapshot", TestSnapshot, TestSnapshot_Cleanup},
	{"TestSnapshotRestore", TestSnapshotRestore, TestSnapshotRestore_Cleanup},
}

func usage(prog_name string) {
	if strings.HasPrefix(prog_name, "/tmp/go-build") {
		fmt.Println("Usage: go run test.go [start|clean]")
		fmt.Println("    start: Start testing")
		fmt.Println("    clean: cleanup test environment")
	} else {
		fmt.Println("Usage: ", prog_name, " [start|clean]")
		fmt.Println("    start: Start testing")
		fmt.Println("    clean: cleanup test environment")
	}
}

func main() {
	if len(os.Args) < 2 {
		usage(os.Args[0])
		return
	}

	if os.Args[1] == "start" {
		startTime := time.Now()
		fmt.Println("Start Time: ", startTime.Format("2006-01-02 15:04:05"))

		for _, test := range tests {
			start := time.Now()
			if err := test.fnExec(); err != nil {
				panic(fmt.Sprintf("[%s] Test failed. err: %v", test.name, err))
			}
			fmt.Printf("========================================\n%s took %s\n\n", test.name, time.Since(start))
		}

		for i := 2; i <= numTests; i++ {
			if err := Cleanup(); err != nil {
				fmt.Printf("Cleanup failed. err: %v", err)
			}

			fmt.Println("Sleep 3 seconds then test next round.")
			time.Sleep(3 * time.Second)
			fmt.Printf("======== ROUND %d =============== \n", i)
			for _, test := range tests {
				start := time.Now()
				if err := test.fnExec(); err != nil {
					panic(fmt.Sprintf("[%s] Test failed. err: %v", test.name, err))
				}
				fmt.Printf("========================================\nROUND%d %s took %s\n\n", i, test.name, time.Since(start))
			}
		}

		stopTime := time.Now()
		fmt.Printf("Result: PASS\n\n")
		fmt.Println("Finish Time: ", stopTime.Format("2006-01-02 15:04:05"))
		fmt.Println("Elapsed Time: ", time.Since(startTime))

	} else if os.Args[1] == "clean" {
		if err := Cleanup(); err != nil {
			fmt.Printf("Cleanup failed. err: %v", err)
		}
	} else {
		usage(os.Args[0])
	}
}

func Cleanup() error {
	fmt.Println("[Cleanup]")
	protocol, _ = getTestProtocol()

	for i := len(tests) - 1; i >= 0; i-- {
		test := tests[i]
		if test.fnClean != nil {
			start := time.Now()
			if err := test.fnClean(); err != nil {
				return fmt.Errorf("[%s] Clean failed. err: %v", test.name, err)
			}
			fmt.Printf("========================================\nClean %s took %s\n\n", test.name, time.Since(start))
		}
	}

	return nil
}

func DeployCSIDriver() error {
	fmt.Println("[DeployCSIDriver]")

	if _, err := execCmd("kubectl wait pods -l app=xevo-csi-controller -n qsan --for=condition=Ready --timeout=600s 2>/dev/null"); err == nil {
		fmt.Printf("CSI Driver was already deployed\n\n")
		csi_exist = true
		return nil
	}

	fmt.Println("Install CSI Driver")
	if _, err := execCmd("../../deploy/install.sh"); err != nil {
		return fmt.Errorf("intall.sh failed.\n")
	}

	fmt.Println("Wait CSI controller plugin ready")
	if _, err := execCmd("kubectl wait pods -l app=xevo-csi-controller -n qsan --for=condition=Ready --timeout=600s"); err != nil {
		return fmt.Errorf("kubectl wait controller plugin failed. err: %v\n", err)
	}

	fmt.Println("Wait CSI node plugin ready")
	if _, err := execCmd("kubectl wait pods -l app=xevo-csi-node -n qsan --for=condition=Ready --timeout=600s"); err != nil {
		return fmt.Errorf("kubectl wait node plugin failed. err: %v\n", err)
	}

	return nil
}

func DeployCSIDriver_Cleanup() error {
	fmt.Println("[DeployCSIDriver_Cleanup]")

	if !csi_exist {
		fmt.Println("Uninstall CSI Driver")
		if _, err := execCmd("../../deploy/uninstall.sh"); err != nil {
			return fmt.Errorf("uninstall.sh failed\n")
		}
	}

	return nil
}

func DeployStorageClass() error {
	fmt.Println("[DeployStorageClass]")

	if _, err := execCmd("kubectl get sc qtest-xevo-storage"); err != nil {
		if mpio {
			if _, err := execCmd("kubectl create -f yaml/sc-m.yaml"); err != nil {
				return fmt.Errorf("Deploy sc-m.yaml failed. err: %v\n", err)
			}
		} else {
			if _, err := execCmd("kubectl create -f yaml/sc.yaml"); err != nil {
				return fmt.Errorf("Deploy sc.yaml failed. err: %v\n", err)
			}
		}
	}

	if protocol, err = getTestProtocol(); err != nil {
		return err
	}

	return nil
}

func DeployStorageClass_Cleanup() error {
	fmt.Println("[DeployStorageClass_Cleanup]")

	if mpio {
		if _, err := execCmd("kubectl delete -f yaml/sc-m.yaml --ignore-not-found=true"); err != nil {
			return fmt.Errorf("Delete sc-m.yaml failed. err: %v\n", err)
		}
	} else {
		if _, err := execCmd("kubectl delete -f yaml/sc.yaml --ignore-not-found=true"); err != nil {
			return fmt.Errorf("Delete sc.yaml failed. err: %v\n", err)
		}
	}

	return nil
}

func DeploySnapshotClass() error {
	fmt.Println("[DeploySnapshotClass]")

	if _, err := execCmd("kubectl get volumesnapshotclass qtest-xevo-snapclass"); err != nil {
		if _, err := execCmd("kubectl create -f yaml/snapclass.yaml"); err != nil {
			return fmt.Errorf("Deploy snapclass.yaml failed. err: %v\n", err)
		}
	}

	return nil
}

func DeploySnapshotClass_Cleanup() error {
	fmt.Println("[DeploySnapshotClass_Cleanup]")

	if _, err := execCmd("kubectl delete -f yaml/snapclass.yaml --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete snapclass.yaml failed. err: %v\n", err)
	}

	return nil
}

func CreatePVC() error {
	fmt.Printf("[CreatePVC][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	fmt.Println("Create test PVC")
	if _, err := execCmd("kubectl create -f yaml/pvc.yaml"); err != nil {
		return fmt.Errorf("Deploy test pvc failed. err: %v\n", err)
	}

	fmt.Println("Wait test PVC ready")
	if _, err := execCmd("kubectl wait --for=jsonpath='{.status.phase}'=Bound --timeout=180s pvc/pvc-test -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait test-pvc failed. err: %v\n", err)
	}

	return nil
}

func DeployPod() error {
	fmt.Printf("[DeployPod][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	fmt.Println("Create test Pod")
	if protocol == "iscsi" || protocol == "fc" {
		if _, err := execCmd("kubectl create -f yaml/pod.yaml"); err != nil {
			return fmt.Errorf("Deploy test pod failed. err: %v\n", err)
		}
	} else {
		if _, err := execCmd("kubectl create -f yaml/pod-file.yaml"); err != nil {
			return fmt.Errorf("Deploy test pod failed. err: %v\n", err)
		}
	}

	fmt.Println("Wait test pod ready")
	if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=900s pod/test-pod -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait test-pod failed. err: %v\n", err)
	}

	if protocol == "nfs" {
		fmt.Println("Check if PVC pvc-rwm-block and pvc-rwo-block are Pending")
		if out, err := execCmd("kubectl get pvc pvc-rwm-block -n qtest -o jsonpath='{.status.phase}'"); err != nil {
			return fmt.Errorf("Get PVC pvc-rwm-block status failed, err: %v\n", err)
		} else if out != "Pending" {
			return fmt.Errorf("PVC pvc-rwm-block status is not Pending. Status: %s\n", out)
		}

		if out, err := execCmd("kubectl get pvc pvc-rwo-block -n qtest -o jsonpath='{.status.phase}'"); err != nil {
			return fmt.Errorf("Get PVC pvc-rwo-block status failed, err: %v\n", err)
		} else if out != "Pending" {
			return fmt.Errorf("PVC pvc-rwo-block status is not Pending. Status: %s\n", out)
		}

		// If test pass, delete PVC pvc-rwm-block and pvc-rwo-block to avoid unnecessary warning message in controller driver during subsequent testing.
		if _, err := execCmd("kubectl delete pvc pvc-rwm-block -n qtest"); err != nil {
			fmt.Printf("Warning! Delete PVC pvc-rwm-block failed. err: %v\n", err)
		}
		if _, err := execCmd("kubectl delete pvc pvc-rwo-block -n qtest"); err != nil {
			fmt.Printf("Warning! Delete PVC pvc-rwo-block failed. err: %v\n", err)
		}
	}

	return nil
}

func CreatePVC_Cleanup() error {
	fmt.Printf("[CreatePVC_Cleanup][%s]\n", protocol)

	if _, err := execCmd("kubectl delete -f yaml/pvc.yaml --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete test PVC failed. err: %v\n", err)
	}

	fmt.Println("Sleep for 10 seconds to ensure that all PVs are deleted.")
	time.Sleep(10 * time.Second)

	return nil
}

func DeployPod_Cleanup() error {
	fmt.Printf("[DeployPod_Cleanup][%s]\n", protocol)

	if protocol == "nfs" {
		if _, err := execCmd("kubectl delete -f yaml/pod-file.yaml --ignore-not-found=true"); err != nil {
			return fmt.Errorf("Delete test pod failed. err: %v\n", err)
		}
	} else {
		if _, err := execCmd("kubectl delete -f yaml/pod.yaml --ignore-not-found=true"); err != nil {
			return fmt.Errorf("Delete test pod failed. err: %v\n", err)
		}
	}

	fmt.Println("Sleep for 30 seconds to ensure that all PVs are deleted.")
	time.Sleep(30 * time.Second)

	return nil
}

func TestAccess() error {
	fmt.Printf("[TestAccess][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	fmt.Println("Test ReadWriteMany with rw")
	if _, err := execCmd("kubectl exec test-pod -n qtest -- touch /pvc-rwm-file-rw/ReadWriteMany"); err != nil {
		return fmt.Errorf("Test ReadWriteMany with rw failed. err: %v\n", err)
	}

	fmt.Println("Test ReadWriteMany with ro")
	if _, err := execCmd("kubectl exec test-pod -n qtest -- touch /pvc-rwm-file-ro/ReadWriteMany"); err == nil {
		return fmt.Errorf("Test ReadWriteMany with ro failed.\n")
	}

	fmt.Println("Test ReadWriteOnce with rw")
	if _, err := execCmd("kubectl exec test-pod -n qtest -- touch /pvc-rwo-file-rw/ReadWriteOnce"); err != nil {
		return fmt.Errorf("Test ReadWriteOnce with rw failed. err: %v\n", err)
	}

	fmt.Println("Test ReadWriteOnce with ro")
	if _, err := execCmd("kubectl exec test-pod -n qtest -- touch /pvc-rwo-file-ro/ReadWriteOnce"); err == nil {
		return fmt.Errorf("Test ReadWriteOnce with ro failed.\n")
	}

	if protocol == "iscsi" || protocol == "fc" {
		fmt.Println("Test ReadWriteMany block device")
		if _, err := execCmd("kubectl exec test-pod -n qtest -- ls /dev/sdxa"); err != nil {
			return fmt.Errorf("Test ReadWriteMany block device failed. err: %v\n", err)
		} else {
			if _, err := execCmd("kubectl exec test-pod -n qtest -- dd if=/dev/zero of=/dev/sdxa bs=512 count=1"); err != nil {
				return fmt.Errorf("Test ReadWriteMany block device dd failed. err: %v\n", err)
			}
		}

		fmt.Println("Test ReadWriteOnce block device")
		if _, err := execCmd("kubectl exec test-pod -n qtest -- ls /dev/sdxb"); err != nil {
			return fmt.Errorf("Test ReadWriteOnce block device failed. err: %v\n", err)
		} else {
			if _, err := execCmd("kubectl exec test-pod -n qtest --  mke2fs /dev/sdxb"); err != nil {
				return fmt.Errorf("Test ReadWriteOnce block device mke2fs failed. err: %v\n", err)
			}
		}
	}

	return nil
}

func TestExpansion() error {
	fmt.Printf("[TestExpansion][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	fmt.Println("Extend pvc-extend size to 21G")
	// '{"spec":{"resources":{"requests":{"storage":"21Gi"}}}}'
	if _, err := execCmd("kubectl patch pvc pvc-extend -n qtest -p '{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"21Gi\"}}}}'"); err != nil {
		return fmt.Errorf("kubectl patch pvc pvc-extend failed. err: %v\n", err)
	}

	fmt.Println("Extend pvc-rwm-file-rw size to 22G")
	// '{"spec":{"resources":{"requests":{"storage":"22Gi"}}}}'
	if _, err := execCmd("kubectl patch pvc pvc-rwm-file-rw -n qtest -p '{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"22Gi\"}}}}'"); err != nil {
		return fmt.Errorf("kubectl patch pvc pvc-rwm-file-rw failed. err: %v\n", err)
	}

	retries := 0
RETRY1:
	fmt.Println("Sleep 3 seconds")
	time.Sleep(3 * time.Second)
	out, err := execCmd("kubectl get pvc pvc-extend -n qtest -o jsonpath='{.spec.resources.requests.storage}'")
	if err != nil {
		return fmt.Errorf("wait failed, err: %v\n", err)
	} else {
		if out == "21Gi" {
			// pass
		} else {
			if retries < 10 {
				retries++
				goto RETRY1
			} else {
				return fmt.Errorf("pvc-extend is not extend to 15G\n")
			}
		}
	}

	fmt.Println("\nSleep 60 seconds")
	time.Sleep(60 * time.Second)
	retries = 0
RETRY2:
	out, err = execCmd("kubectl exec test-pod -n qtest -- df -hP /pvc-rwm-file-rw | grep pvc-rwm-file-rw | awk '{print $2}'")
	if err != nil {
		return fmt.Errorf("wait failed, err: %v\n", err)
	} else {
		if size := gbsize2uint(out); size >= 20 {
			// pass
		} else {
			if retries < 30 {
				fmt.Printf("pvc-rwm-file-rw filesystem sizeGB: %v. Sleep 10 seconds then try again!\n", size)
				time.Sleep(10 * time.Second)
				retries++
				goto RETRY2
			} else {
				return fmt.Errorf("pvc-rwm-file-rw is not extend to 22G\n")
			}
		}
	}

	return nil
}

func TestWebService() error {
	fmt.Printf("[TestWebService][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	fmt.Println("Create nginx pod")
	if _, err := execCmd("kubectl create -f yaml/nginx.yaml"); err != nil {
		return fmt.Errorf("Deploy nginx pod failed. err: %v\n", err)
	}

	// Just an enhancement as it sometimes fails with pods 'web-0' not found, especially on VMware VM workers.
	if _, err := execCmd("kubectl get pod/web-0 -n qtest --no-headers"); err != nil {
		fmt.Println("Sleep 1 second to wait for pod/web-0 to be created...")
		time.Sleep(1 * time.Second)
	}

	fmt.Println("Wait nginx pod ready")
	if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=600s pod/web-0 -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait pod/web-0 failed. err: %v\n", err)
	}

	fmt.Println("Generate index.html")
	if _, err := execCmd("kubectl exec web-0 -n qtest -- bash -c 'echo TEST1 > /usr/share/nginx/html/index.html'"); err != nil {
		return fmt.Errorf("Failed to generate index.html. err: %v\n", err)
	}

	// ip := getMasterIp()
	nodeName := getPodRunningNodeName("web-0")
	nodeIp := getNodeInternalIP(nodeName)
	fmt.Printf("Check web service on node(%s) via %s\n", nodeName, nodeIp)
	retries := 0
RETRY:
	if out, err := getWebData(nodeIp + ":30009"); err != nil {
		if retries < 10 {
			fmt.Printf("%v. Sleep 3 seconds then try again!\n", err)
			retries++
			time.Sleep(3 * time.Second)
			goto RETRY
		}
		return fmt.Errorf("Failed to get web service data. err: %v\n", err)
	} else {
		if out != "TEST1" {
			return fmt.Errorf("Incorrect web service data. data: %s\n", out)
		}
	}

	return nil
}

func TestWebService_Cleanup() error {
	fmt.Printf("[TestWebService_Cleanup][%s]\n", protocol)

	fmt.Println("Delete nginx pod")
	if _, err := execCmd("kubectl delete -f yaml/nginx.yaml --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete nginx pod failed. err: %v\n", err)
	}

	fmt.Println("Delete nginx pvc-nginx-web-0")
	if _, err := execCmd("kubectl delete pvc pvc-nginx-web-0 -n qtest --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete nginx pvc-nginx-web-0 failed. err: %v\n", err)
	}

	return nil
}

func TestSnapshot() error {
	fmt.Printf("[TestSnapshot][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	if _, err := execCmd("kubectl create -f yaml/snap-pvc.yaml"); err != nil {
		return fmt.Errorf("Create yaml/snap-pvc.yaml failed. err: %v\n", err)
	}

	yamlSnapContent, err := ioutil.ReadFile("yaml/snap-pvc-snap-tmpl.yaml")
	if err != nil {
		log.Fatalf("Can't read yaml/snap-pvc-snap-tmpl: %v", err)
	}
	yamlSnap := string(yamlSnapContent)

	const cnt = 256
	var snapname string
	snap := "testsnap-pvc-snap%d"
	for i := 1; i <= cnt; i = i + 1 {
		if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSnap, i)), "create", "-f", "-"); err != nil {
			return fmt.Errorf("Create VolumeSnapshot(%s) failed. err: %v\n", fmt.Sprintf(snap, i), err)
		}

		if _, err := execCmd("kubectl wait --for=jsonpath='{.status.readyToUse}'=true --timeout=180s -n qtest volumesnapshot/" + fmt.Sprintf(snap, i)); err != nil {
			return fmt.Errorf("kubectl wait volumesnapshot/%s failed. err: %v\n", fmt.Sprintf(snap, i), err)
		}
	}

	// Delete snapshot 2. It will be in trash.
	snapname = fmt.Sprintf(snap, 2)
	if _, err := execCmd(fmt.Sprintf("kubectl delete vs %s -n qtest", snapname)); err != nil {
		return fmt.Errorf("Failed to delete VolumeSnapshot %s. err: %v\n", snapname, err)
	}

	// Create snapshot 257, it should fail because of LVMERR_TOO_MANY_SNAPS.
	snapname = fmt.Sprintf(snap, cnt+1)
	if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSnap, cnt+1)), "create", "-f", "-"); err != nil {
		return fmt.Errorf("Failed to create VolumeSnapshot %s. err: %v\n", snapname, err)
	}

	if _, err := execCmd("kubectl wait --for=jsonpath='{.status.readyToUse}'=true --timeout=120s -n qtest volumesnapshot/" + snapname); err != nil {
		fmt.Printf("kubectl wait volumesnapshot/%s failed. err: %v\n", snapname, err)
		fmt.Printf("Correct! It should fail.\n\n")
	} else {
		return fmt.Errorf("It should fail to create VolumeSnapshot %s.\n", snapname)
	}

	// Delete snapshot 1
	snapname = fmt.Sprintf(snap, 1)
	if _, err := execCmd(fmt.Sprintf("kubectl delete vs %s -n qtest", snapname)); err != nil {
		return fmt.Errorf("Failed to delete VolumeSnapshot %s. err: %v\n", snapname, err)
	}

	// Create snapshot 258. It should succeed because snapshot 1 & 2 will be cleaned (force delete)
	if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSnap, 258)), "create", "-f", "-"); err != nil {
		return fmt.Errorf("Create VolumeSnapshot(%s) failed. err: %v\n", fmt.Sprintf(snap, 258), err)
	}
	if _, err := execCmd("kubectl wait --for=jsonpath='{.status.readyToUse}'=true --timeout=180s -n qtest volumesnapshot/" + fmt.Sprintf(snap, 258)); err != nil {
		return fmt.Errorf("kubectl wait volumesnapshot/%s failed. err: %v\n", fmt.Sprintf(snap, 258), err)
	}

	// This time, snapshot 258 should succeed because snapshot 1 & 2 will be cleaned (force delete)
	if _, err := execCmd("kubectl wait --for=jsonpath='{.status.readyToUse}'=true --timeout=300s -n qtest volumesnapshot/" + fmt.Sprintf(snap, 257)); err != nil {
		return fmt.Errorf("kubectl wait volumesnapshot/%s failed. err: %v\n", fmt.Sprintf(snap, 257), err)
	}

	// Create snapshot 260, it should fail because of LVMERR_TOO_MANY_SNAPS.
	snapname = fmt.Sprintf(snap, 260)
	if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSnap, 260)), "create", "-f", "-"); err != nil {
		return fmt.Errorf("Create VolumeSnapshot(%s) failed. err: %v\n", snapname, err)
	}

	if _, err := execCmd("kubectl wait --for=jsonpath='{.status.readyToUse}'=true --timeout=120s -n qtest volumesnapshot/" + snapname); err != nil {
		fmt.Printf("kubectl wait volumesnapshot/%s failed. err: %v\n", snapname, err)
		fmt.Printf("Correct! It should fail.\n\n")

		commands := []string{
			fmt.Sprintf("kubectl patch vs %s --type json --patch='[ { \"op\": \"remove\", \"path\": \"/metadata/finalizers\" } ]' -n qtest", snapname),
			fmt.Sprintf("kubectl delete vs %s -n qtest --wait=false", snapname),
		}
		execCmds(commands)

		out, err := execCmd(fmt.Sprintf("kubectl get vsc -n qtest | grep -w %s | awk '{print $1}'", snapname))
		if err != nil {
			return fmt.Errorf("Failed to get VolumeSnapshotContent %s. err: %v\n", snapname, err)
		}

		vscName := strings.Trim(string(out), "\n")
		fmt.Printf("vscName: %s \n", vscName)
		commands = []string{
			fmt.Sprintf("kubectl patch vsc %s --type json --patch='[ { \"op\": \"remove\", \"path\": \"/metadata/finalizers\" } ]' -n qtest", vscName),
			fmt.Sprintf("kubectl delete vsc %s -n qtest --wait=false", vscName),
		}
		execCmds(commands)

	} else {
		return fmt.Errorf("It should fail to create VolumeSnapshot %s.\n", snapname)
	}

	return nil
}

func TestSnapshot_Cleanup() error {
	fmt.Printf("[TestSnapshot_Cleanup][%s]\n", protocol)

	const cnt = 260
	snap := "testsnap-pvc-snap%d"
	for i := 1; i <= cnt; i++ {
		if _, err := execCmd(fmt.Sprintf("kubectl delete vs %s -n qtest --ignore-not-found=true", fmt.Sprintf(snap, i))); err != nil {
			return fmt.Errorf("Failed to delete VolumeSnapshot %s. err: %v\n", fmt.Sprintf(snap, i), err)
		}
	}

	if _, err := execCmd("kubectl delete -f yaml/snap-pvc.yaml --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete yaml/snap-pvc.yaml failed. err: %v\n", err)
	}

	return nil
}

func TestSnapshotRestore() error {
	fmt.Printf("[TestSnapshotRestore][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	fmt.Println("Create restore source pvc/pod")
	if _, err := execCmd("kubectl create -f yaml/restore-source.yaml"); err != nil {
		return fmt.Errorf("Deploy yaml/restore-source.yaml failed. err: %v\n", err)
	}

	fmt.Println("Wait restore source-pvc ready")
	if _, err := execCmd("kubectl wait --for=jsonpath='{.status.phase}'=Bound --timeout=180s pvc/restore-source-pvc -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait pvc/restore-source-pvc failed. err: %v\n", err)
	}

	fmt.Println("Wait restore source-pod ready")
	if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=300s pod/restore-source-pod -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait pod/restore-source-pod failed. err: %v\n", err)
	}

	yamlSnapContent, err := ioutil.ReadFile("yaml/restore-source-snap-tmpl.yaml")
	if err != nil {
		log.Fatalf("Can't read yaml/restore-source-snap-tmpl.yaml: %v", err)
	}
	yamlSnap := string(yamlSnapContent)

	yamlRestoreContent, err := ioutil.ReadFile("yaml/restore-tmpl.yaml")
	if err != nil {
		log.Fatalf("Can't read yaml/restore-tmpl.yaml: %v", err)
	}
	yamlRestore := string(yamlRestoreContent)

	for i := 1; i <= numRestoreTests; i++ {
		testPattern := fmt.Sprintf("Qsan Restore %d", i)
		if _, err := execCmd("kubectl exec restore-source-pod -n qtest -- sh -c 'echo " + testPattern + " > /test-pv/TEST'"); err != nil {
			return fmt.Errorf("Failed to generate TEST file. err: %v\n", err)
		}

		if _, err := execCmd("kubectl exec restore-source-pod -n qtest -- sync"); err != nil {
			return fmt.Errorf("Failed to sync source-pod filesystem. err: %v\n", err)
		}

		// if protocol == "nfs" {
		// 	fmt.Println("Sleep for 5 seconds to ensure test pattern is synced.")
		// 	time.Sleep(5 * time.Second)
		// }

		fmt.Printf("Create snapshot %d\n", i)
		if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSnap, i)), "create", "-f", "-"); err != nil {
			return fmt.Errorf("Create snapshot %d failed. err: %v\n", i, err)
		}

		fmt.Printf("Restore snapshot %d\n", i)
		if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlRestore, i, i, i, i)), "create", "-f", "-"); err != nil {
			return fmt.Errorf("Restore snapshot %d failed. err: %v\n", i, err)
		}

		restorePod := fmt.Sprintf("test-restore-pod%d", i)
		fmt.Printf("Wait restore pod(%s) ready\n", restorePod)
		if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=900s pod/" + restorePod + " -n qtest"); err != nil {
			return fmt.Errorf("kubectl wait pod/%s failed. err: %v\n", restorePod, err)
		}

		out, err := execCmd("kubectl exec " + restorePod + " -n qtest -- cat /test-pv/TEST")
		if err != nil {
			return fmt.Errorf("Failed to cat /test-pv/TEST of %s pod. err: %v\n", restorePod, err)
		}

		if strings.TrimRight(out, "\n") != testPattern {
			return fmt.Errorf("%s data mismatch. (%s vs %s)\n", restorePod, strings.TrimRight(out, "\n"), testPattern)
		}
	}

	return nil
}

func TestSnapshotRestore_Cleanup() error {
	fmt.Printf("[TestSnapshotRestore_Cleanup][%s]\n", protocol)

	for i := 1; i <= numRestoreTests; i++ {
		restorePod := fmt.Sprintf("test-restore-pod%d", i)
		if _, err := execCmd("kubectl delete pod " + restorePod + " -n qtest --ignore-not-found=true"); err != nil {
			return fmt.Errorf("kubectl wait pod/%s failed. err: %v\n", restorePod, err)
		}

		restorePVC := fmt.Sprintf("restore-source-pvc-restore%d", i)
		if _, err := execCmd("kubectl delete pvc " + restorePVC + " -n qtest --ignore-not-found=true"); err != nil {
			return fmt.Errorf("kubectl wait pvc/%s failed. err: %v\n", restorePVC, err)
		}

		snap := fmt.Sprintf("restore-source-pvc-snap%d", i)
		if _, err := execCmd("kubectl delete vs " + snap + " -n qtest --ignore-not-found=true"); err != nil {
			return fmt.Errorf("kubectl wait volumesnapshot/%s failed. err: %v\n", snap, err)
		}
	}

	fmt.Println("Delete restore source pvc/pod")
	if _, err := execCmd("kubectl delete -f yaml/restore-source.yaml --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete yaml/restore-source.yaml failed. err: %v\n", err)
	}

	return nil
}

func TestClone() error {
	fmt.Printf("[TestClone][%s]\n", protocol)
	if protocol, err = ensureProtocol(protocol); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var firstErr error
	errChan := make(chan error, numCloneTests)

	for i := 1; i <= numCloneTests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			fmt.Printf("Create test%d source pvc & pod\n", i)
			yamlSourceContent, err := ioutil.ReadFile("yaml/clone-source-tmpl.yaml")
			if err != nil {
				log.Fatalf("Can't read yaml/clone-source-tmpl.yaml: %v", err)
			}
			yamlSource := string(yamlSourceContent)
			if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSource, i, i, i)), "create", "-f", "-"); err != nil {
				errChan <- fmt.Errorf("Create test%d source pvc & pod failed. err: %v\n", i, err)
				return
			}

			podName := fmt.Sprintf("clone-source-pod%d", i)
			if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=900s -n qtest pod/" + podName); err != nil {
				errChan <- fmt.Errorf("kubectl wait source pod/%s failed. err: %v\n", podName, err)
				return
			}

			pattern := fmt.Sprintf("Qsan%d", i)
			if _, err := execCmd("kubectl exec " + podName + " -n qtest -- sh -c 'echo " + pattern + " > /test-pv/TEST'"); err != nil {
				errChan <- fmt.Errorf("Failed to generate TEST file in Pod(%s). err: %v\n", podName, err)
				return
			}

			if _, err := execCmd("kubectl exec " + podName + " -n qtest -- sync"); err != nil {
				errChan <- fmt.Errorf("Failed to sync Pod(%s) filesystem. err: %v\n", podName, err)
				return
			}

			// if protocol == "nfs" {
			// 	fmt.Println("Sleep for 5 seconds to ensure test pattern is synced.")
			// 	time.Sleep(5 * time.Second)
			// }

			fmt.Printf("Create test%d clone pvc & pod\n", i)
			yamlCloneContent, err := ioutil.ReadFile("yaml/clone-tmpl.yaml")
			if err != nil {
				log.Fatalf("Can't read yaml/clone-tmpl.yaml: %v", err)
			}
			yamlClone := string(yamlCloneContent)
			if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlClone, i, i, i, i)), "create", "-f", "-"); err != nil {
				errChan <- fmt.Errorf("Create test%d clone pvc & pod failed. err: %v\n", i, err)
				return
			}

			clonePodName := fmt.Sprintf("test-clone-pod%d", i)
			if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=2400s -n qtest pod/" + clonePodName); err != nil {
				errChan <- fmt.Errorf("kubectl wait clone pod/%s failed. err: %v\n", clonePodName, err)
				return
			}

			out, err := execCmd("kubectl exec " + clonePodName + " -n qtest -- cat /test-pv/TEST")
			if err != nil {
				errChan <- fmt.Errorf("Failed to cat /test-pv/TEST of %s pod. err: %v\n", clonePodName, err)
			}

			if strings.TrimRight(out, "\n") != pattern {
				errChan <- fmt.Errorf("Test%d data mismatch. (%s vs %s)\n", i, strings.TrimRight(out, "\n"), pattern)
			}

		}(i)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		fmt.Printf("Error: %v\n", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func TestClone_Cleanup() error {
	fmt.Printf("[TestClone_Cleanup][%s]\n", protocol)

	yamlSourceContent, err := ioutil.ReadFile("yaml/clone-source-tmpl.yaml")
	if err != nil {
		log.Fatalf("Can't read yaml/clone-source-tmpl.yaml: %v", err)
	}
	yamlSource := string(yamlSourceContent)

	yamlCloneContent, err := ioutil.ReadFile("yaml/clone-tmpl.yaml")
	if err != nil {
		log.Fatalf("Can't read yaml/clone-tmpl.yaml: %v", err)
	}
	yamlClone := string(yamlCloneContent)

	for i := 1; i <= numCloneTests; i++ {
		fmt.Printf("Delete test%d clone pod & pvc\n", i)
		if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlClone, i, i, i, i)), "delete", "--ignore-not-found=true", "-f", "-"); err != nil {
			return fmt.Errorf("Delete test%d clone pod & pvc failed. err: %v\n", i, err)
		}

		fmt.Printf("Delete test%d source pod & pvc\n", i)
		if _, err := execCmdEx("kubectl", strings.NewReader(fmt.Sprintf(yamlSource, i, i, i)), "delete", "--ignore-not-found=true", "-f", "-"); err != nil {
			return fmt.Errorf("Delete test%d source pod & pvc failed. err: %v\n", i, err)
		}
	}

	return nil
}

func execCmd(cmd string) (string, error) {
	fmt.Printf("> %s\n", cmd)
	//out, err := exec.Command("bash", "-c", cmd).CombinedOutput()
	//fmt.Printf("%+v\n", string(out))
	//if err != nil {
	//	return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
	//}

	//return string(out), err
	var stdout, stderr bytes.Buffer
	command := exec.Command("bash", "-c", cmd)
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()

	if stdout.Len() > 0 {
		fmt.Printf("[stdout]\n%s\n", stdout.String())
	}
	if stderr.Len() > 0 {
		fmt.Printf("[stderr]\n%s\n", stderr.String())
	}

	if err != nil {
		// 把 stderr 回傳在 error message 裡，避免只得到空的 err
		return stdout.String(), fmt.Errorf("%v: %s", err, stderr.String())
	}

	return stdout.String(), nil
}

func execCmdEx(name string, reader *strings.Reader, args ...string) (string, error) {
	fmt.Printf("> %s, args=%+v \n", name, args)
	cmd := exec.Command(name, args...)
	cmd.Stdin = reader
	out, err := cmd.CombinedOutput()
	fmt.Printf("%+v\n", string(out))
	if err != nil {
		return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
	}

	return string(out), err
}

// func execCmds(commands []string) {
// 	var wg sync.WaitGroup
// 	ch := make(chan struct{}) // Control the execution order of commands

// 	for _, command := range commands {
// 		wg.Add(1)
// 		go func(cmd string) {
// 			defer wg.Done()
// 			<-ch
// 			if _, err := execCmd(cmd); err != nil {
// 				fmt.Printf("%s failed. err: %v\n", cmd, err)
// 			}
// 		}(command)
// 		ch <- struct{}{}
// 	}
// 	wg.Wait()
// }

func execCmds(commands []string) {
	for _, cmd := range commands {
		if _, err := execCmd(cmd); err != nil {
			fmt.Printf("%s failed. err: %v\n", cmd, err)
		}
	}
}

func getWebData(url string) (string, error) {
	fmt.Println("> http://" + url)
	response, err := http.Get("http://" + url)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	fmt.Printf("%s\n", string(body))
	return strings.TrimRight(string(body), "\n"), nil
}

func gbsize2uint(s string) float64 {
	size, _ := strconv.ParseFloat(strings.TrimRight(s, "G\n"), 64)
	return size
}

func getMasterIp() string {
	if out, err := execCmd("kubectl get node -o wide | grep control-plane | awk '{print $6}'"); err != nil {
		fmt.Printf("Failed to get master node IP. err: %v\n", err)
		return "127.0.0.1"
	} else {
		return strings.TrimRight(out, "\n")
	}
}

func getPodRunningNodeName(podName string) string {
	if out, err := execCmd("kubectl get pod " + podName + " -n qtest --template {{.spec.nodeName}}"); err != nil {
		fmt.Printf("Failed to get Pod(%s) node name. err: %v\n", podName, err)
		return ""
	} else {
		return strings.TrimRight(out, "\n")
	}
}

func getNodeInternalIP(nodeName string) string {
	if out, err := execCmd("kubectl get node " + nodeName + " -o jsonpath='{.status.addresses[?(@.type==\"InternalIP\")].address}'"); err != nil {
		fmt.Printf("Failed to get Node(%s) INTERNAL-IP. err: %v\n", nodeName, err)
		return ""
	} else {
		return strings.TrimRight(out, "\n")
	}
}

func ensureProtocol(p string) (string, error) {
	if p != "" {
		return p, nil
	}
	return getTestProtocol()
}

func getTestProtocol() (string, error) {
	protocol, err := execCmd("kubectl get sc qtest-xevo-storage -o jsonpath='{.parameters.protocol}'")
	if err != nil {
		return "", fmt.Errorf("Get protocol from qtest-xevo-storage StorageClass failed, err: %v\n", err)
	} else {
		if protocol == "iscsi" || protocol == "fc" || protocol == "nfs" {
			fmt.Println("Test protocol: ", protocol)
			return protocol, nil
		} else {
			return protocol, fmt.Errorf("Unknown protocol(%s) in qtest-xevo-storage StorageClass\n", protocol)
		}
	}
}
