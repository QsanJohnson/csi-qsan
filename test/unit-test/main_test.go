package main

import (
	"fmt"
	"testing"
	"time"
)

func TestMain(t *testing.T) {
	startTime := time.Now()
	fmt.Println("Start Time: ", startTime.Format("2006-01-02 15:04:05"))

	if err := DeployCSIDriver(); err != nil {
		t.Fatalf("DeployCSIDriver failed. err: %v", err)
	}

	if err := DeployStorageClass(); err != nil {
		t.Fatalf("DeployStorageClass failed. err: %v", err)
	}

	if err := DeployPod(); err != nil {
		t.Fatalf("DeployPod failed. err: %v", err)
	}

	if err := TestAccess(); err != nil {
		t.Errorf("TestAccess failed. err: %v", err)
	}

	if err := TestExpansion(); err != nil {
		t.Errorf("TestExpansion failed. err: %v", err)
	}

	if err := TestWebService(); err != nil {
		t.Errorf("TestWebService failed. err: %v", err)
	}

	fmt.Println("\n\nSleep 60 seconds to start cleanup ...")
	time.Sleep(60 * time.Second)
	if err := Cleanup(); err != nil {
		t.Errorf("Cleanup failed. err: %v", err)
	}

	stopTime := time.Now()
	fmt.Println("Finish Time: ", stopTime.Format("2006-01-02 15:04:05"))
	fmt.Println("Elapsed Time: ", time.Since(startTime))
}
