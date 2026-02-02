package main

import (
	"fmt"
	"testing"
	"time"
)

func TestMain(t *testing.T) {
	startTime := time.Now()
	fmt.Println("Start Time: ", startTime.Format("2006-01-02 15:04:05"))

	for _, test := range tests {
		start := time.Now()
		if err := test.fnExec(); err != nil {
			t.Fatalf("[%s] Test failed. err: %v", test.name, err)
		}
		fmt.Printf("========================================\n%s took %s\n\n", test.name, time.Since(start))
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
