// @2022 QSAN Inc. All right reserved

package goiscsi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
)

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func writeDeviceFile(devFile, content string) error {
	data := []byte(content)
	return os.WriteFile(devFile, data, 0644)
}

func execCmd(name string, args ...string) (string, error) {
	klog.V(5).Infof("[execCmd] %s, args=%+v", name, args)
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	klog.V(5).Infof("[execCmd] Output ==>\n%+v", string(out))
	if err != nil {
		return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
	}

	return string(out), err
}

func execCmdContext(ctx context.Context, name string, args ...string) (string, error) {
	klog.V(5).Infof("[execCmdContext] %s, args=%+v \n", name, args)
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	klog.V(5).Infof("[execCmdContext] Output ==>\n%+v\n", string(out))
	if err != nil {
		return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
	}

	return string(out), err
}

func sessionFieldValue(s string) string {
	_, value := fieldKeyValue(s, ":")
	return value
}

func fieldKeyValue(s string, sep string) (string, string) {
	var key, value string
	tokens := strings.SplitN(s, sep, 2)
	if len(tokens) > 0 {
		key = strings.Trim(strings.TrimSpace(tokens[0]), sep)
	}
	if len(tokens) > 1 {
		value = replaceEmpty(strings.TrimSpace(tokens[1]))
	}
	return key, value
}

func replaceEmpty(s string) string {
	if s == "<empty>" {
		return ""
	}
	return s
}
