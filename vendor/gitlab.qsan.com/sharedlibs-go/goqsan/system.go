// @2022 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"net/http"
)

// SystemOp handles system related methods of the QSAN storage.
type SystemOp struct {
	client *Client
}

// The response data of GetAbout method
type AboutData struct {
	Addresses []struct {
		Address string `json:"address"`
		Online  bool   `json:"online"`
	} `json:"addresses"`
	SystemName   string `json:"systemName"`
	FirmwareVer  string `json:"firmwareVer"`
	ModelName    string `json:"modelName"`
	ModelType    string `json:"modelType"`
	SerialNumber string `json:"serialNumber"`
	Wwn          string `json:"wwn"`
	Os           string `json:"os"`
}

// NewSystem returns system operation
func NewSystem(client *Client) *SystemOp {
	return &SystemOp{client}
}

// GetAbout get system information without authentication
func (s *SystemOp) GetAbout(ctx context.Context) (*AboutData, error) {
	req, err := s.client.NewRequest(ctx, http.MethodGet, "/rest/v1/about", nil)
	if err != nil {
		return nil, err
	}

	res := AboutData{}
	if err := s.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}
