package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

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

		if err := DeployCSIDriver(); err != nil {
			panic(fmt.Sprintf("DeployCSIDriver failed. err: %v", err))
		}

		if err := DeployStorageClass(); err != nil {
			panic(fmt.Sprintf("DeployStorageClass failed. err: %v", err))
		}

		if err := DeployPod(); err != nil {
			panic(fmt.Sprintf("DeployPod failed. err: %v", err))
		}

		if err := TestAccess(); err != nil {
			fmt.Printf("TestAccess failed. err: %v", err)
		}

		if err := TestExpansion(); err != nil {
			fmt.Printf("TestExpansion failed. err: %v", err)
		}

		if err := TestWebService(); err != nil {
			fmt.Printf("TestWebService failed. err: %v", err)
		}

		stopTime := time.Now()
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

func DeployCSIDriver() error {
	fmt.Println("[DeployCSIDriver]")

	fmt.Println("Install CSI Driver")
	if _, err := execCmd("../deploy/install.sh"); err != nil {
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

func DeployStorageClass() error {
	fmt.Println("[DeployStorageClass]")
	if _, err := execCmd("kubectl create -f yaml/sc.yaml"); err != nil {
		return fmt.Errorf("Deploy sc.yaml failed. err: %v\n", err)
	}

	return nil
}

func DeployPod() error {
	fmt.Println("[DeployPod]")

	fmt.Println("Create test Pod")
	if _, err := execCmd("kubectl create -f yaml/pod.yaml"); err != nil {
		return fmt.Errorf("Deploy test pod failed. err: %v\n", err)
	}

	fmt.Println("Wait test pod ready")
	if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=600s pod/test-pod -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait test-pod failed. err: %v\n", err)
	}

	return nil
}

func TestAccess() error {
	fmt.Println("[TestAccess]")

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

	return nil
}

func TestExpansion() error {
	fmt.Println("[TestExpansion]")

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
	out, err = execCmd("kubectl exec test-pod -n qtest -- df -h /pvc-rwm-file-rw | grep pvc-rwm-file-rw | awk '{print $2}'")
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
	fmt.Println("[TestWebService]")

	fmt.Println("Create nginx pod")
	if _, err := execCmd("kubectl create -f yaml/nginx.yaml"); err != nil {
		return fmt.Errorf("Deploy nginx pod failed. err: %v\n", err)
	}

	fmt.Println("Wait nginx pod ready")
	if _, err := execCmd("kubectl wait --for=condition=Ready --timeout=600s pod/web-0 -n qtest"); err != nil {
		return fmt.Errorf("kubectl wait pod/web-0 failed. err: %v\n", err)
	}

	fmt.Println("Generate index.html")
	if _, err := execCmd("kubectl exec web-0 -n qtest -- bash -c 'echo TEST1 > /usr/share/nginx/html/index.html'"); err != nil {
		return fmt.Errorf("Failed to generate index.html. err: %v\n", err)
	}

	ip := getMasterIp()
	fmt.Println("Check web service")
	retries := 0
RETRY:
	if out, err := getWebData(ip + ":30009"); err != nil {
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

func Cleanup() error {
	fmt.Println("[Cleanup]")

	fmt.Println("Delete nginx pod")
	if _, err := execCmd("kubectl delete -f yaml/nginx.yaml --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete nginx pod failed. err: %v\n", err)
	}

	fmt.Println("Delete nginx pvc-nginx-web-0")
	if _, err := execCmd("kubectl delete pvc pvc-nginx-web-0 -n qtest --ignore-not-found=true"); err != nil {
		return fmt.Errorf("Delete nginx pvc-nginx-web-0 failed. err: %v\n", err)
	}

	fmt.Println("Delete test pod")
	if _, err := execCmd("kubectl delete -f yaml/pod.yaml --ignore-not-found=true"); err != nil {
		// fmt.Printf("Delete test pod failed. err: %v\n", err)
		return fmt.Errorf("Delete test pod failed. err: %v\n", err)
	}

	fmt.Println("Delete StorageClass")
	if _, err := execCmd("kubectl delete -f yaml/sc.yaml --ignore-not-found=true"); err != nil {
		// fmt.Printf("Delete sc.yaml failed. err: %v\n", err)
		return fmt.Errorf("Delete sc.yaml failed. err: %v\n", err)
	}

	fmt.Println("Uninstall CSI Driver")
	if _, err := execCmd("../deploy/uninstall.sh"); err != nil {
		// fmt.Printf("uninstall.sh failed\n")
		return fmt.Errorf("uninstall.sh failed\n")
	}

	return nil
}

func execCmd(cmd string) (string, error) {
	fmt.Printf("> %s\n", cmd)
	out, err := exec.Command("bash", "-c", cmd).CombinedOutput()
	fmt.Printf("%+v\n", string(out))
	if err != nil {
		return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
	}

	return string(out), err
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
