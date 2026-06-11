// @2022 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
)

// VolumeOp handles volume related methods of the QSAN storage.
type VolumeOp struct {
	SnapshotOp
	QoSOp
	client *AuthClient
	model  string
}

type VolumeMetadata struct {
	Status    string `json:"status,omitempty"`
	Type      string `json:"type,omitempty"`
	Content   string `json:"content,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type VolumeData struct {
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	PoolID                string `json:"poolId"`
	LunID                 string `json:"lunId"`
	TargetID              string `json:"targetId"`
	ReplicaTargetID       string `json:"replicaTargetId"`
	ReplicaTargetName     string `json:"replicaTargetName"`
	Online                bool   `json:"online"`
	State                 string `json:"state"`
	Progress              int    `json:"progress"`
	Health                string `json:"health"`
	Provision             string `json:"provision"`
	TotalSize             uint64 `json:"totalSize"`
	UsedSize              uint64 `json:"usedSize"`
	BlockSize             uint64 `json:"blockSize"`
	StripeSize            uint64 `json:"stripeSize"`
	CacheMode             string `json:"cacheMode"`
	IoPriority            string `json:"ioPriority"`
	BgIoPriority          string `json:"bgIoPriority"`
	EnableReadAhead       bool   `json:"enableReadAhead"`
	EraseData             string `json:"eraseData"`
	EnableFastRaidRebuild bool   `json:"enableFastRaidRebuild"`
	TargetResponseTime    uint64 `json:"targetResponseTime"`
	MaxIops               uint64 `json:"maxIops"`
	MaxThroughtput        uint64 `json:"maxThroughput"`
	Tags                  struct {
		Wwn  string `json:"wwn"`
		Type string `json:"type"`
	} `json:"tags"`
	Metadata VolumeMetadata `json:"metadata"`
}

type VolumeCreateOptions struct {
	Name            string         `json:"name"`
	TotalSize       uint64         `json:"totalSize"`
	BlockSize       uint64         `json:"blockSize"`
	PoolID          string         `json:"poolId"`
	IoPriority      string         `json:"ioPriority,omitempty"`
	BgIoPriority    string         `json:"bgIoPriority,omitempty"`
	CacheMode       string         `json:"cacheMode,omitempty"`
	EnableReadAhead *bool          `json:"enableReadAhead,omitempty"`
	Metadata        VolumeMetadata `json:"metadata,omitempty"`
}

//Patch /rest/v2/storage/block/volumes/_volumes
type Tag struct {
	Type string `json:"type,omitempty"`
}

//Patch /rest/v2/storage/block/volumes/_volumes
type VolumeModifyOptions struct {
	VolumeQoSOptions
	Name            string         `json:"name,omitempty"`
	TotalSize       uint64         `json:"totalSize,omitempty"`
	BgIoPriority    string         `json:"bgIoPriority,omitempty"`
	CacheMode       string         `json:"cacheMode,omitempty"`
	EnableReadAhead *bool          `json:"enableReadAhead,omitempty"`
	Tags            Tag            `json:"tags,omitempty"`
	Metadata        VolumeMetadata `json:"metadata,omitempty"`
}

// NewVolume returns volume operation
func NewVolume(client *AuthClient) *VolumeOp {
	
	sysOp := NewSystem(&client.Client)
	info, _ := sysOp.GetAbout(context.Background())
	if info.ModelType == "SAN" {
		fmt.Println("Use SAN VolumeOp")
		return &VolumeOp{SnapshotOp{client}, QoSOp{client}, client, ModelSAN}
	} else {
		fmt.Println("Use QSM4 VolumeOp")
		return &VolumeOp{SnapshotOp{client}, QoSOp{client}, client, ModelQSM4}
	}
}

// ListVolumes list all volumes
func (v *VolumeOp) ListVolumes(ctx context.Context) (*[]VolumeData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/block/volumes", nil)
	if err != nil {
		return nil, err
	}

	res := []VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListVolumeByID list a dedicated volume with volId
func (v *VolumeOp) ListVolumeByID(ctx context.Context, volId string) (*VolumeData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/block/volumes/"+volId, nil)
	if err != nil {
		return nil, err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		// resterr, ok := err.(*RestError)
		// if ok {
		// 	fmt.Printf("[ListVolumeByID] StatusCode=%d ErrResp=%+v\n", resterr.StatusCode, resterr.ErrResp)
		// }
		return nil, err
	}
	return &res, nil
}

// list volumes under given PoolID
func (v *VolumeOp) ListVolumesByPoolID(ctx context.Context, poolId string) (*[]VolumeData, error) {

	var query string
	if v.model == ModelSAN {
		query = fmt.Sprintf("q=poolId=%s", poolId)
	} else {
		query = fmt.Sprintf("poolId=%s", poolId)
	}

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/block/volumes?"+query, nil)
	if err != nil {
		return nil, err
	}

	res := []VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// CreateVolume create a volume on a storage container
func (v *VolumeOp) CreateVolume(ctx context.Context, poolId, volname string, volsize uint64, options *VolumeCreateOptions) (*VolumeData, error) {

	options.PoolID = poolId
	options.Name = volname
	options.TotalSize = volsize

	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/storage/block/volumes", string(rawdata))
	if err != nil {
		return nil, err
	}
	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// DeleteVolume delete a volume from a storage container
func (v *VolumeOp) DeleteVolume(ctx context.Context, volId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/storage/block/volumes/"+volId, nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

func (v *VolumeOp) ModifyVolume(ctx context.Context, volId string, options *VolumeModifyOptions) (*VolumeData, error) {
	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/block/volumes/"+volId, string(rawdata))
	if err != nil {
		return nil, err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Get metadata Timestamp
func (v *VolumeOp) GetTimestamp(ctx context.Context, volId string) (string, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/block/volumes/"+volId, nil)
	if err != nil {
		return "", err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", err
	}
	return res.Metadata.Timestamp, nil
}

// update metadata Timestamp
func (v *VolumeOp) SetTimestamp(ctx context.Context, volId, timestamp string) (string, error) {

	param := &VolumeModifyOptions{
		Metadata: VolumeMetadata{
			Timestamp: timestamp,
		},
	}
	rawdata, _ := json.Marshal(param)

	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/block/volumes/"+volId, string(rawdata))
	if err != nil {
		return "", err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", err
	}
	return res.Metadata.Timestamp, nil
}

// Get metadata
func (v *VolumeOp) GetMetadata(ctx context.Context, volId string) (metastatus, metatype string, metacontent []byte, err error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/block/volumes/"+volId, nil)
	if err != nil {
		return "", "", nil, err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", "", nil, err
	}

	rawDecodedText, _ := b64.StdEncoding.DecodeString(res.Metadata.Content)

	return res.Metadata.Status, res.Metadata.Type, []byte(rawDecodedText), nil
}

// Update metadata
func (v *VolumeOp) SetMetadata(ctx context.Context, volId, metastatus, metatype string, metacontent []byte) (string, string, []byte, error) {

	metacontent64 := b64.StdEncoding.EncodeToString(metacontent)
	param := &VolumeModifyOptions{
		Metadata: VolumeMetadata{
			Status:  metastatus,
			Type:    metatype,
			Content: metacontent64,
		},
	}
	rawdata, _ := json.Marshal(param)

	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/block/volumes/"+volId, string(rawdata))
	if err != nil {
		return "", "", nil, err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", "", nil, err
	}

	rawDecodedText, _ := b64.StdEncoding.DecodeString(res.Metadata.Content)

	return res.Metadata.Status, res.Metadata.Type, []byte(rawDecodedText), nil
}

func (v *VolumeOp) Clone(ctx context.Context, volId, snapId, newVolName, poolId string) (*VolumeData, error) {
	m := map[string]string{
		"volumeName": newVolName,
		"poolId":     poolId,
	}
	if snapId != "" {
		m["snapshotId"] = snapId
	}
	rawdata, _ := json.Marshal(m)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/storage/block/volumes/"+volId+"/clone", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := VolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (v *VolumeOp) DeleteCloneTask(ctx context.Context, volId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/backup/replicate/"+volId+"/task", nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

