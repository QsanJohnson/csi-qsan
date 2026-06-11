// @2025 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"encoding/json"
	"net/http"
)

type QoSOp struct {
	client *AuthClient
}

// return value of GET /rest/v2/storage/qos/volumes
// Patch /rest/v2/storage/qos/volumes
type QoSData struct {
	EnableQos bool   `json:"enableQos"`
	QosRule   string `json:"qosRule"`
}

type VolumeQoSOptions struct {
	IoPriority         string `json:"ioPriority,omitempty"`
	TargetResponseTime uint64 `json:"targetResponseTime,omitempty"`
	MaxIops            uint64 `json:"maxIops,omitempty"`
	MaxThroughtput     uint64 `json:"maxThroughput,omitempty"`
}

func (v *QoSOp) GetQoS(ctx context.Context) (*QoSData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/qos/volumes", nil)
	if err != nil {
		return nil, err
	}

	res := QoSData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (v *QoSOp) SetQoS(ctx context.Context, qosEnable bool, qosRule string) (*QoSData, error) {

	options := QoSData{}
	options.EnableQos = qosEnable
	options.QosRule = qosRule

	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/qos/volumes", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := QoSData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
