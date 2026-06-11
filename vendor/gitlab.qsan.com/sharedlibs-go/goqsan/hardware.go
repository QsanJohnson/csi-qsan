// @2023 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"net/http"
)

// HardwareOp handles hardware related methods of the QSAN storage.
type HardwareOp struct {
	client *AuthClient
}

// The response data of GetHardware method
type HardwareData struct {
	EncID int `json:"encId"`
	Info  struct {
		SysHealth             string   `json:"sysHealth"`
		CPUInfo               string   `json:"cpuInfo"`
		CPUUsage              int      `json:"cpuUsage"`
		MemoryInfo            []string `json:"memoryInfo"`
		MemTotal              int      `json:"memTotal"`
		MemUsed               int      `json:"memUsed"`
		ControllerNum         int      `json:"controllerNum"`
		BackplaneID           string   `json:"backplaneID"`
		BackplaneSerialNumber string   `json:"backplaneSerialNumber"`
		McuVersion            string   `json:"mcuVersion"`
		SystemUptime          int      `json:"systemUptime"`
		SerialNumber          string   `json:"serialNumber"`
		ModelName             string   `json:"modelName"`
		MoreInfo              struct {
			Voltage []struct {
				Name   string `json:"name"`
				Value  int    `json:"value"`
				Status string `json:"status"`
			} `json:"voltage"`
			Temperature []struct {
				Name   string `json:"name"`
				Value  int    `json:"value"`
				Status string `json:"status"`
			} `json:"temperature"`
			PsuStatus []struct {
				Name   string `json:"name"`
				Status string `json:"status"`
			} `json:"psuStatus"`
			FanSpeed []struct {
				Name   string `json:"name"`
				Value  int    `json:"value"`
				Status string `json:"status"`
			} `json:"fanSpeed"`
			CacheToFlash []struct {
				Name   string `json:"name"`
				Status string `json:"status"`
			} `json:"cacheToFlash"`
		} `json:"moreInfo"`
	} `json:"info"`
}

// NewHardware returns hardware operation
func NewHardware(client *AuthClient) *HardwareOp {
	return &HardwareOp{client}
}

// GetHardware get hardware information
func (s *HardwareOp) GetHardware(ctx context.Context) (*HardwareData, error) {
	req, err := s.client.NewRequest(ctx, http.MethodGet, "/rest/v1/hardware/0/info?moreInfo=true", nil)
	if err != nil {
		return nil, err
	}

	res := HardwareData{}
	if err := s.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}
