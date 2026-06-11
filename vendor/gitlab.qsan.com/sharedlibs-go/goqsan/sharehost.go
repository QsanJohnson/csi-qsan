// @2025 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// SharehostOp handles sharehost related methods of the QSAN storage.
type SharehostOp struct {
	client *AuthClient
}

type NfsRules struct {
	Host       string   `json:"host"`
	Security   []string `json:"security"`
	Permission string   `json:"permission"`
	AsyncWrite bool     `json:"asyncWrite"`
	RootSquash bool     `json:"rootSquash"`
}

type SharehostData struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	ShareHosts     []string `json:"shareHosts"`
	ConnectedShare []struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	} `json:"connectedShare"`
	Services []string   `json:"services"`
	NfsRules []NfsRules `json:"nfsRules"`
}

type SharehostCreateOptions struct {
	Name          string     `json:"name"`
	AllowHostList []string   `json:"allowHostList"`
	ShareIDList   []string   `json:"shareIdList,omitempty"`
	Services      []string   `json:"services,omitempty"`
	NfsRules      []NfsRules `json:"nfsRules,omitempty"`
}

type SharehostModifyOptions struct {
	Name          string     `json:"name,omitempty"`
	AllowHostList []string   `json:"allowHostList"`
	ShareIDList   []string   `json:"shareIdList"`	// Don't add omitempty to avoid [] becomes nil
	NfsRules      []NfsRules `json:"nfsRules"`
}

type KeytabData struct {
	Enable  bool `json:"enable"`
	Keytabs []struct {
		Version     int    `json:"version"`
		Principal   string `json:"principal"`
		EncryptType string `json:"encryptType"`
	} `json:"keytabs"`
}

// NewSharehost returns sharehost operation
func NewSharehost(client *AuthClient) *SharehostOp {
	return &SharehostOp{client}
}

// List all Sharehosts or certain sharehost by sharehost name
func (v *SharehostOp) ListSharehosts(ctx context.Context, sharehostName string) (*[]SharehostData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/sharehosts", nil)
	if err != nil {
		return nil, err
	}
	tmpres := []SharehostData{}
	if err := v.client.SendRequest(ctx, req, &tmpres); err != nil {
		return nil, err
	}

	if sharehostName == "" {
		return &tmpres, nil
	} else {
		for i := 0; i < len(tmpres); i++ {
			if tmpres[i].Name == sharehostName {
				res := []SharehostData{tmpres[i]}
				return &res, nil
			}
		}
		return nil, fmt.Errorf("Sharehost name(%s) not found.", sharehostName)
	}
}

// List certain sharehost by sharehostID
func (v *SharehostOp) ListSharehostByID(ctx context.Context, sharehostID string) (*SharehostData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/sharehosts/"+sharehostID, nil)
	if err != nil {
		return nil, err
	}
	res := SharehostData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil

}

// CreateTarget create a target on a storage server
func (v *SharehostOp) CreateSharehost(ctx context.Context, name string, services []string, param *SharehostCreateOptions) (*SharehostData, error) {

	param.Name = name
	param.Services = services
	rawdata, _ := json.Marshal(param)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/dataTransfer/sharehosts", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := SharehostData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Patch certain sharehost
func (v *SharehostOp) ModifySharehost(ctx context.Context, sharehostID string, param *SharehostModifyOptions) (*SharehostData, error) {
	rawdata, _ := json.Marshal(param)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/dataTransfer/sharehosts/"+sharehostID, string(rawdata))
	if err != nil {
		return nil, err
	}
	res := SharehostData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Put certain sharehost about adding shares
func (v *SharehostOp) AddSharehostShares(ctx context.Context, sharehostID string, shares []string) error {
	body := map[string]interface{} {
		"shareIdList": shares,
	}
	rawdata, _ := json.Marshal(body)
	req, err := v.client.NewRequest(ctx, http.MethodPut, "/rest/v2/dataTransfer/sharehosts/"+sharehostID+"/shares", string(rawdata))
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// Delete certain sharehost about removing shares
func (v *SharehostOp) RemoveSharehostShares(ctx context.Context, sharehostID string, shares []string) error {
	body := map[string]interface{} {
		"shareIdList": shares,
	}
	rawdata, _ := json.Marshal(body)
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/dataTransfer/sharehosts/"+sharehostID+"/shares", string(rawdata))
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// Patch certain sharehost about adding services
func (v *SharehostOp) AddSharehostServices(ctx context.Context, sharehostID string, services []string) (*SharehostData, error) {
	body := map[string]interface{} {
		"services": services,
	}
	rawdata, _ := json.Marshal(body)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/dataTransfer/sharehosts/"+sharehostID+"/addService", string(rawdata))
	if err != nil {
		return nil, err
	}
	res := SharehostData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Patch certain sharehost about removing services
func (v *SharehostOp) RemoveSharehostServices(ctx context.Context, sharehostID string, services []string) (*SharehostData, error) {
	body := map[string]interface{} {
		"services": services,
	}
	rawdata, _ := json.Marshal(body)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/dataTransfer/sharehosts/"+sharehostID+"/removeService", string(rawdata))
	if err != nil {
		return nil, err
	}
	res := SharehostData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Delete sharehost
func (v *SharehostOp) DeleteSharehost(ctx context.Context, sharehostID string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/dataTransfer/sharehosts/"+sharehostID, nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// Get NFS keytab information
func (v *SharehostOp) GetKeytab(ctx context.Context) (*KeytabData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/dataTransfer/protocol/nfs/keytab", nil)
	if err != nil {
		return nil, err
	}

	res := KeytabData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}