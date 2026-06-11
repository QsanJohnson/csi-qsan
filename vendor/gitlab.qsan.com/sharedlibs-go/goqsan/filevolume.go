// @2025 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"net/http"
)

// FileVolumeOp handles file volume related methods of the QSAN storage.
type FileVolumeOp struct {
	SnapshotOp
	QoSOp
	client *AuthClient
}

type FileVolumeMetadata struct {
	Status    string `json:"status,omitempty"`
	Type      string `json:"type,omitempty"`
	Content   string `json:"content,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type FileVolumeData struct {
	ID                 string             `json:"id"`
	Name               string             `json:"name"`
	PoolID             string             `json:"poolId"`
	PoolName           string             `json:"poolName"`
	ReplicaTargetID    string             `json:"replicaTargetId"`
	ReplicaTargetName  string             `json:"replicaTargetName"`
	Type               string             `json:"type"`
	Online             bool               `json:"online"`
	State              string             `json:"status"` // The value of status and state are the same
	Health             string             `json:"health"`
	Provision          string             `json:"provision"`
	Progress           int                `json:"progress"`
	TotalSize          uint64             `json:"totalSize"`
	UsedSize           uint64             `json:"usedSize"`
	BlockSize          uint64             `json:"blockSize"`
	VideoEditing       bool               `json:"videoEditing"`
	CacheMode          string             `json:"cacheMode"`
	IoPriority         string             `json:"ioPriority"`
	BgIoPriority       string             `json:"bgIoPriority"`
	EnableReadAhead    bool               `json:"enableReadAhead"`
	VdoEnabled         bool               `json:"vdoEnabled"`
	TargetResponseTime uint64             `json:"targetResponseTime"`
	MaxIops            uint64             `json:"maxIops"`
	MaxThroughtput     uint64             `json:"maxThroughput"`
	Metadata           FileVolumeMetadata `json:"metadata"`
}

type FileVolumeCreateOptions struct {
	Name            string         `json:"name"`
	TotalSize       uint64         `json:"totalSize"`
	BlockSize       uint64         `json:"blockSize"`
	PoolID          string         `json:"poolId"`
	IoPriority      string         `json:"ioPriority,omitempty"`
	BgIoPriority    string         `json:"bgIoPriority,omitempty"`
	CacheMode       string         `json:"cacheMode,omitempty"`
	EnableReadAhead *bool          `json:"enableReadAhead,omitempty"`
	Batch           int            `json:"batch,omitempty"`
	Metadata        VolumeMetadata `json:"metadata,omitempty"`
}

type FileVolumeModifyOptions struct {
	VolumeQoSOptions
	Type            string         `json:"type,omitempty"`
	TotalSize       uint64         `json:"totalSize,omitempty"`
	BgIoPriority    string         `json:"bgIoPriority,omitempty"`
	CacheMode       string         `json:"cacheMode,omitempty"`
	EnableReadAhead *bool          `json:"enableReadAhead,omitempty"`
	Metadata        VolumeMetadata `json:"metadata,omitempty"`
}

type ShareData struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	Health           string `json:"health"`
	PoolID           string `json:"poolId"`
	PoolName         string `json:"poolName"`
	FileVolumeID     string `json:"fileVolumeId"`
	FileVolumeName   string `json:"fileVolumeName"`
	TotalSize        uint64 `json:"totalSize"`
	UsedSize         uint64 `json:"usedSize"`
	IsOrphan         bool   `json:"isOrphan"`
	EnableUserhome   bool   `json:"enableUserhome"`
	EnableRecycleBin bool   `json:"enableRecycleBin"`
	EnableWorm       bool   `json:"enableWorm"`
	WormDuration     int    `json:"wormDuration"`
	QuotaLimit       uint64 `json:"quotaLimit"`
	QuotaUsed        uint64 `json:"quotaUsed"`
}

type UserGroupPermission struct {
	ID              string `json:"id"`
	Type            string `json:"type,omitempty"`
	Name            string `json:"name,omitempty"`
	GroupPermission string `json:"groupPermission,omitempty"`
	Permission      string `json:"permission"`
}

type SharePermission struct {
	UserLocal  []UserGroupPermission `json:"user:local"`
	GroupLocal []UserGroupPermission `json:"group:local"`
}

type ShareCreateOptions struct {
	FileVolumeID     string          `json:"fileVolumeId"`
	Name             string          `json:"name"`
	Description      string          `json:"description,omitempty"`
	EnableUserhome   bool            `json:"enableUserhome,omitempty"`
	Permissions      SharePermission `json:"permissions,omitempty"`
	EnableRecycleBin bool            `json:"enableRecycleBin,omitempty"`
	QuotaLimit       uint64          `json:"quotaLimit"`
}

type ShareModifyOptions struct {
	Name             string `json:"name,omitempty"`
	Description      string `json:"description,omitempty"`
	EnableUserhome   bool   `json:"enableUserhome,omitempty"`
	EnableRecycleBin bool   `json:"enableRecycleBin,omitempty"`
	QuotaLimit       uint64 `json:"quotaLimit"`
}

type ShareSetOptions struct {
	Permissions SharePermission `json:"permissions"`
}

// NewFileVolume returns file volume operation
func NewFileVolume(client *AuthClient) *FileVolumeOp {
	return &FileVolumeOp{SnapshotOp{client}, QoSOp{client}, client}
}

// ListVolumes list all file volumes
func (v *FileVolumeOp) ListVolumes(ctx context.Context) (*[]FileVolumeData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/file/volumes", nil)
	if err != nil {
		return nil, err
	}

	res := []FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListVolumeByID list a dedicated file volume using volId
func (v *FileVolumeOp) ListVolumeByID(ctx context.Context, volId string) (*FileVolumeData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/file/volumes/"+volId, nil)
	if err != nil {
		return nil, err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		// resterr, ok := err.(*RestError)
		// if ok {
		// 	fmt.Printf("[ListVolumeByID] StatusCode=%d ErrResp=%+v\n", resterr.StatusCode, resterr.ErrResp)
		// }
		return nil, err
	}
	return &res, nil
}

// ListVolumesByPoolID list file volumes under given poolId
func (v *FileVolumeOp) ListVolumesByPoolID(ctx context.Context, poolId string) (*[]FileVolumeData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/file/volumes?poolId="+poolId, nil)
	if err != nil {
		return nil, err
	}

	res := []FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// CreateVolume create a file volume on a storage pool
func (v *FileVolumeOp) CreateVolume(ctx context.Context, poolId, volname string, volsize uint64, options *FileVolumeCreateOptions) (*FileVolumeData, error) {
	options.PoolID = poolId
	options.Name = volname
	options.TotalSize = volsize

	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/storage/file/volumes", string(rawdata))
	if err != nil {
		return nil, err
	}
	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// DeleteVolume delete a file volume using volId
func (v *FileVolumeOp) DeleteVolume(ctx context.Context, volId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/storage/file/volumes/"+volId, nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// ModifyVolume modify a volume's properties
func (v *FileVolumeOp) ModifyVolume(ctx context.Context, volId string, options *FileVolumeModifyOptions) (*FileVolumeData, error) {
	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/file/volumes/"+volId, string(rawdata))
	if err != nil {
		return nil, err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// ListShares list all shares
func (v *FileVolumeOp) ListShares(ctx context.Context) (*[]ShareData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/shares", nil)
	if err != nil {
		return nil, err
	}

	res := []ShareData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListShareByID list a dedicated share using shareId
func (v *FileVolumeOp) ListShareByID(ctx context.Context, shareId string) (*ShareData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/shares/"+shareId, nil)
	if err != nil {
		return nil, err
	}

	res := ShareData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListShareByVolumeID list shares under given volId
func (v *FileVolumeOp) ListShareByVolumeID(ctx context.Context, volId string) (*[]ShareData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/shares?volumeId="+volId, nil)
	if err != nil {
		return nil, err
	}

	res := []ShareData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// CreateShare create a share with a file volume
func (v *FileVolumeOp) CreateShare(ctx context.Context, fvolId, name string, quota uint64, options *ShareCreateOptions) (*ShareData, error) {
	options.FileVolumeID = fvolId
	options.Name = name
	options.QuotaLimit = quota

	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/shares", string(rawdata))
	if err != nil {
		return nil, err
	}
	res := ShareData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// RecreateShare recreate a orphan share
func (v *FileVolumeOp) RecreateShare(ctx context.Context, sharename string) (*ShareData, error) {
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/shares/"+sharename+"/recreate", nil)
	if err != nil {
		return nil, err
	}
	res := ShareData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// DeleteShare delete a share
func (v *FileVolumeOp) DeleteShare(ctx context.Context, shareId string) error {
	req, err := v.client.NewRequest(ctx, http.MethodDelete, "/rest/v2/shares/"+shareId, nil)
	if err != nil {
		return err
	}

	res := EmptyData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return err
	}

	return nil
}

// ModifyShare modify a share's properties
func (v *FileVolumeOp) ModifyShare(ctx context.Context, shareId string, options *ShareModifyOptions) (*ShareData, error) {
	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/shares/"+shareId, string(rawdata))
	if err != nil {
		return nil, err
	}

	res := ShareData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// GetSharePermission get the permission of a share
func (v *FileVolumeOp) GetSharePermission(ctx context.Context, shareId string) (*SharePermission, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/shares/"+shareId+"/permission", nil)
	if err != nil {
		return nil, err
	}

	res := SharePermission{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// EditSharePermission set the permission of a share
func (v *FileVolumeOp) EditSharePermission(ctx context.Context, shareId string, options *ShareSetOptions) (*SharePermission, error) {
	rawdata, _ := json.Marshal(options)
	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/shares/"+shareId+"/permission/edit", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := SharePermission{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// GetTimestamp get metadata timestamp of a file volume
func (v *FileVolumeOp) GetTimestamp(ctx context.Context, volId string) (string, error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/file/volumes/"+volId, nil)
	if err != nil {
		return "", err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", err
	}
	return res.Metadata.Timestamp, nil
}

// SetTimestamp update metadata timestamp of a file volume
func (v *FileVolumeOp) SetTimestamp(ctx context.Context, volId, timestamp string) (string, error) {
	param := &FileVolumeModifyOptions{
		Metadata: VolumeMetadata{
			Timestamp: timestamp,
		},
	}
	rawdata, _ := json.Marshal(param)

	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/file/volumes/"+volId, string(rawdata))
	if err != nil {
		return "", err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", err
	}
	return res.Metadata.Timestamp, nil
}

// GetMetadata get metadata of a file volume
func (v *FileVolumeOp) GetMetadata(ctx context.Context, volId string) (metastatus, metatype string, metacontent []byte, err error) {
	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/file/volumes/"+volId, nil)
	if err != nil {
		return "", "", nil, err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", "", nil, err
	}

	rawDecodedText, _ := b64.StdEncoding.DecodeString(res.Metadata.Content)

	return res.Metadata.Status, res.Metadata.Type, []byte(rawDecodedText), nil
}

// SetMetadata update metadata of a file volume
func (v *FileVolumeOp) SetMetadata(ctx context.Context, volId, metastatus, metatype string, metacontent []byte) (string, string, []byte, error) {
	metacontent64 := b64.StdEncoding.EncodeToString(metacontent)
	param := &FileVolumeModifyOptions{
		Metadata: VolumeMetadata{
			Status:  metastatus,
			Type:    metatype,
			Content: metacontent64,
		},
	}
	rawdata, _ := json.Marshal(param)

	req, err := v.client.NewRequest(ctx, http.MethodPatch, "/rest/v2/storage/file/volumes/"+volId, string(rawdata))
	if err != nil {
		return "", "", nil, err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return "", "", nil, err
	}

	rawDecodedText, _ := b64.StdEncoding.DecodeString(res.Metadata.Content)

	return res.Metadata.Status, res.Metadata.Type, []byte(rawDecodedText), nil
}

// Clone perform a local file volume clone on the same pool
func (v *FileVolumeOp) Clone(ctx context.Context, volId, snapId, newVolName, poolId string) (*FileVolumeData, error) {
	m := map[string]string{
		"volumeName": newVolName,
		"poolId":     poolId,
	}
	if snapId != "" {
		m["snapshotId"] = snapId
	}
	rawdata, _ := json.Marshal(m)
	req, err := v.client.NewRequest(ctx, http.MethodPost, "/rest/v2/storage/file/volumes/"+volId+"/clone", string(rawdata))
	if err != nil {
		return nil, err
	}

	res := FileVolumeData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// DeleteCloneTask delete Qreplica task of a file volume
func (v *FileVolumeOp) DeleteCloneTask(ctx context.Context, volId string) error {
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
