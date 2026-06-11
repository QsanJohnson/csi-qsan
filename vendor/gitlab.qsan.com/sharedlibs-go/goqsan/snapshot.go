// @2025 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"encoding/json"
	"net/http"
)

type SnapshotOp struct {
	client *AuthClient
}

// Patch /rest/v2/backup/snapshot/targets/_volumeID
type SnapshotMutableSetting struct {
	ProtectionGroup string `json:"protectionGroup,omitempty"`
	TotalSize       int    `json:"totalSize,omitempty"`
}

// return value of GET /rest/v2/backup/snapshot/targets/_volumeID
// return value of PATCH /rest/v2/backup/snapshot/targets/_volumeID
type SnapshotSetting struct {
	Type              string `json:"type"`
	SnapshotMaxPolicy struct {
		MaxLimit uint64 `json:"maxLimit"`
		Policy   string `json:"policy"`
	} `json:"snapshotMaxPolicy"`
	SnapshotMutableSetting
	AvailableSize int `json:"availableSize"`
	MinimumSize   int `json:"minimumSize"`
	SuggestSize   int `json:"suggestSize"`
}

// SnapExpose defines the exposure settings for a snapshot.
type SnapExpose struct {
	Enable    bool   `json:"enable"`
	Mode      string `json:"mode"`
	WriteSize uint64 `json:"writeSize"`
}

// SnapTrash represents the trash status of a snapshot.
type SnapTrash struct {
	InTrash bool `json:"inTrash"`
}

// Patch /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotID
type SnapshotOptions struct {
	Expose SnapExpose `json:"expose"`
	Trash  SnapTrash  `json:"trash"`
}

// return value of GET /rest/v2/backup/snapshot/targets/_volumeID/snapshots
// return value of Post /rest/v2/backup/snapshot/targets/_volumeID/snapshots
// return value of Patch /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotID
type SnapshotData struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	CreateTime int64      `json:"createTime"`
	UsedSize   uint64     `json:"usedSize"`
	Expose     SnapExpose `json:"expose"`
	Trash      SnapTrash  `json:"trash"`
}

// Get Volume snapshot settings
// GET /rest/v2/backup/snapshot/targets/_volumeID
func (v *SnapshotOp) GetSnapshotSetting(ctx context.Context, volId string) (*SnapshotSetting, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/backup/snapshot/targets/"+volId, nil)
	if err != nil {
		return nil, err
	}

	res := SnapshotSetting{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Enable snapshot space
// PATCH /rest/v2/backup/snapshot/targets/_volumeID
func (v *SnapshotOp) SetSnapshotSetting(ctx context.Context, volId string, options *SnapshotMutableSetting) (*SnapshotSetting, error) {

	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/backup/snapshot/targets/"+volId, string(rawdata))
	if err != nil {
		return nil, err
	}

	res := SnapshotSetting{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Create volume snapshot
// POST /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotName
func (v *SnapshotOp) CreateSnapshot(ctx context.Context, volId, snapeName string) (*SnapshotData, error) {

	m := map[string]string{
		"name": snapeName,
	}
	rawdata, _ := json.Marshal(m)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := SnapshotData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// List all volume snapshots
// GET /rest/v2/backup/snapshot/targets/_volumeID/snapshots
func (v *SnapshotOp) ListSnapshots(ctx context.Context, volId string) (*[]SnapshotData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots", nil)
	if err != nil {
		return nil, err
	}

	res := []SnapshotData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Get Volume certain snapshot list
// GET /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotID
func (v *SnapshotOp) GetSnapshot(ctx context.Context, volId, snapId string) (*SnapshotData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots/"+snapId, nil)
	if err != nil {
		return nil, err
	}

	res := SnapshotData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Patch certain volume snapshot
// PATCH /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotID
func (v *SnapshotOp) ModifySnapshot(ctx context.Context, volId, snapId string, options *SnapshotOptions) (*[]SnapshotData, error) {

	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots/"+snapId, string(rawdata))
	if err != nil {
		return nil, err
	}

	res := []SnapshotData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Rollback to certain volume snapshot
// POST /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotID
func (v *SnapshotOp) RollbackSnapshot(ctx context.Context, volId, snapId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots/"+snapId+"/rollback", nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// Delete all volume snapshots
// DELETE /rest/v2/backup/snapshot/targets/_volumeID/snapshots
func (v *SnapshotOp) DeleteAllSnapshots(ctx context.Context, volId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots", nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// Delete certain volume snapshot
// DELETE /rest/v2/backup/snapshot/targets/_volumeID/snapshots/_snapshotID
func (v *SnapshotOp) DeleteSnapshot(ctx context.Context, volId, snapId string, permanent bool) error {
	m := map[string]bool{
		"permanent": permanent,
	}
	rawdata, _ := json.Marshal(m)
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/backup/snapshot/targets/"+volId+"/snapshots/"+snapId, string(rawdata))
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}
