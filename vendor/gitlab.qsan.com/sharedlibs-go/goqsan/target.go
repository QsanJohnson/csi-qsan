// @2022 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// TargetOp handles target related methods of the QSAN storage.
type TargetOp struct {
	client *AuthClient
}

// POST /rest/v2/dataTransfer/targets
// PATCH /rest/v2/dataTransfer/targets/_targetID
type Iscsi struct {
	Name  string      `json:"name,omitempty"`
	Alias interface{} `json:"alias,omitempty"`
	Eths  []string    `json:"eths,omitempty"`
}

// POST /rest/v2/dataTransfer/targets
type CreateTargetParam struct {
	Name   string  `json:"name"`
	Type   string  `json:"type"`
	Iscsis []Iscsi `json:"iscsi"`
}

// PATCH /rest/v2/dataTransfer/targets/_targetID
type TargetParam struct {
	Name   string  `json:"name,omitempty"`
	Type   string  `json:"type,omitempty"`
	Iscsis []Iscsi `json:"iscsi,omitempty"`
}

// POST /rest/v2/dataTransfer/targets/_targetID/luns
// PATCH /rest/v2/dataTransfer/targets/_targetID/luns/_lunID
type Host struct {
	Name string `json:"name,omitempty"` // iqn/WWN
}

// POST /rest/v2/dataTransfer/targets/_targetID/luns
type LunMapParam struct {
	Name     string `json:"name,omitempty"`
	VolumeID string `json:"volumeId"`
	Hosts    []Host `json:"hosts"`
}

// PATCH /rest/v2/dataTransfer/targets/_targetID/luns/_lunID
type LunParam struct {
	Name  string `json:"name,omitempty"`
	Hosts []Host `json:"hosts,omitempty"`
}

// return value GET /rest/v2/dataTransfer/targets
// return value POST /rest/v2/dataTransfer/targets
type TargetData struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
	Fcp  []struct {
		Wwn string `json:"wwn"`
	} `json:"fcp,omitempty"`
	Iscsi []struct {
		Iqn   string      `json:"iqn"`
		Name  string      `json:"name"`
		Alias interface{} `json:"alias"`
		Eths  []string    `json:"eths"`
	} `json:"iscsi,omitempty"`
}

// return value POST /rest/v2/dataTransfer/targets/_targetID/luns
// return value GET /rest/v2/dataTransfer/targets/_targetID/luns/_lunID
// return value PATCH /rest/v2/dataTransfer/targets/_targetID/luns/_lunID
type LunData struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	VolumeID string `json:"volumeId"`
	Hosts    []struct {
		Name string `json:"name"`
		Rule string `json:"rule"`
	} `json:"hosts"`
}

// return value GET /rest/v2/dataTransfer/protocol/fibreChannel
type FCData struct {
	ID           string `json:"id"`
	LinkSpeed    int    `json:"linkSpeed"`
	SupportSpeed []int  `json:"supportSpeed"`
	Topology     string `json:"topology"`
	Wwnn         string `json:"wwnn"`
	Wwpn         string `json:"wwpn"`
	ErrCounter   struct {
		SignalLoss  int `json:"signalLoss"`
		SyncLoss    int `json:"syncLoss"`
		LinkFailure int `json:"linkFailure"`
		InvalidCRC  int `json:"invalidCRC"`
	} `json:"errCounter"`
}

// NewTarget returns volume operation
func NewTarget(client *AuthClient) *TargetOp {
	return &TargetOp{client}
}

// List all Targets or certain target by target name
func (v *TargetOp) ListTargets(ctx context.Context, targetName string) (*[]TargetData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/targets", nil)
	if err != nil {
		return nil, err
	}
	tmpres := []TargetData{}
	if err := v.client.SendRequest(ctx, req, &tmpres); err != nil {
		return nil, err
	}

	if targetName == "" {
		return &tmpres, nil
	} else {
		for i := 0; i < len(tmpres); i++ {
			if tmpres[i].Name == targetName {
				res := []TargetData{tmpres[i]}
				return &res, nil
			}
		}
		return nil, fmt.Errorf("Target name(%s) not found.", targetName)
	}
}

// List certain target by targetID
func (v *TargetOp) ListTargetByID(ctx context.Context, targetID string) (*TargetData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/targets/"+targetID, nil)
	if err != nil {
		return nil, err
	}
	res := TargetData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil

}

// Patch certain target
func (v *TargetOp) ModifyTarget(ctx context.Context, targetID string, param *TargetParam) (*TargetData, error) {
	rawdata, _ := json.Marshal(param)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/dataTransfer/targets/"+targetID, string(rawdata))
	if err != nil {
		return nil, err
	}
	res := TargetData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil

}

// CreateTarget create a target on a storage server
func (v *TargetOp) CreateTarget(ctx context.Context, tgtName, tgtType string, param *CreateTargetParam) (*TargetData, error) {

	param.Name = tgtName
	param.Type = tgtType
	rawdata, _ := json.Marshal(param)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/dataTransfer/targets", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := TargetData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Delete target
func (v *TargetOp) DeleteTarget(ctx context.Context, targetId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/dataTransfer/targets/"+targetId, nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

//Mapping lun
func (v *TargetOp) MapLun(ctx context.Context, targetID, volID string, param *LunMapParam) (*LunData, error) {

	param.VolumeID = volID
	rawdata, _ := json.Marshal(param)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/dataTransfer/targets/"+targetID+"/luns", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := LunData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		// resterr, ok := err.(*RestError)
		// for retries := 0; ok && resterr.StatusCode == 429 && retries < 10; retries++ {
		// 	// This is a workaround for SAN attach LUN issue.
		// 	time.Sleep(1 * time.Second)
		// 	if err := v.client.SendRequest(ctx, req, &res); err == nil {
		// 		return &res, nil
		// 	}
		// 	resterr, ok = err.(*RestError)
		// }
		return nil, err
	}
	return &res, nil
}

//Unmapping lun
func (v *TargetOp) UnmapLun(ctx context.Context, targetId, lunId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/dataTransfer/targets/"+targetId+"/luns/"+lunId, nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

//list all luns under given targetID
func (v *TargetOp) ListAllLuns(ctx context.Context, targetID string) (*[]LunData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/targets/"+targetID+"/luns/", nil)
	if err != nil {
		return nil, err
	}

	res := []LunData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

//list target lun
func (v *TargetOp) ListTargetLun(ctx context.Context, targetID, lunID string) (*LunData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/targets/"+targetID+"/luns/"+lunID, nil)
	if err != nil {
		return nil, err
	}

	res := LunData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

//patch target lun
func (v *TargetOp) ModifyTargetLun(ctx context.Context, targetID, lunID string, param *LunParam) (*LunData, error) {
	rawdata, _ := json.Marshal(param)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/dataTransfer/targets/"+targetID+"/luns/"+lunID, string(rawdata))
	if err != nil {
		return nil, err
	}

	res := LunData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

//list fibre channel
func (v *TargetOp) ListFC(ctx context.Context) (*[]FCData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/protocol/fibreChannel", nil)
	if err != nil {
		return nil, err
	}

	res := []FCData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// //patch target fibre channel
// func (v *TargetOp) ListFC(ctx context.Context, fcID string, param *FCPatchParam) (*[]FCData, error) {
// 	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/protocol/fibreChannel"+fcID, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res := []LunData{}
// 	if err := v.client.SendRequest(ctx, req, &res); err != nil {
// 		return nil, err
// 	}
// 	return &res, nil
// }
