package driver

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.qsan.com/sharedlibs-go/gofc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.qsan.com/sharedlibs-go/goqsan"
	"k8s.io/klog/v2"
)

const (
	ProvisionThin          = "THIN"
	ProvisionThick         = "THICK"
	InvalidReplicaTargetID = "4294967295"
	MAX_VD_NAME_LEN        = 32
	MAX_SNAP_NAME_LEN      = 32
	MIN_SNAPSHOT_SIZE_GB   = 4
)

var (
	iscsiTargetTypes = []string{"iSCSI"}     // Both QSM3 and QSM4 use 'iSCSI'
	fcTargetTypes    = []string{"FCP", "FC"} // QSM3 use 'FCP', and QSM4 use 'FC'
)

const (
	CAP_CACHEMODE_WT     = (1 << 0)
	CAP_READAHEAD_ENABLE = (1 << 1)
	META_VER             = 1
	META_KIND_LOCAL      = 1
	META_KIND_ISCSI      = 2
	META_KIND_FC         = 3
	META_KIND_NFS        = 4
	MCIDX_ATTR           = 5 // MCIDX: META_CONTENT_INDEX
	META_ATTR_RO         = (1 << 0)
	META_ATTR_NEP        = (1 << 1) // Need Erase Parameter
)

type genericVolumeData struct {
	ID                string
	Name              string
	Type              string
	PoolID            string
	TargetID          string
	ReplicaTargetID   string
	ReplicaTargetName string
	LunID             string
	Online            bool
	State             string
	Progress          int
	Health            string
	Provision         string
	TotalSize         uint64
	UsedSize          uint64
}

type GenericVolumeInterface interface {
	ListVolumeByID(ctx context.Context, volId string) (interface{}, error)
	ListVolumesByPoolID(ctx context.Context, poolId string) (interface{}, error)
	ModifyVolume(ctx context.Context, volId string, opts interface{}) (interface{}, error)

	ListSnapshots(ctx context.Context, volId string) (*[]goqsan.SnapshotData, error)
	CreateSnapshot(ctx context.Context, volId, snapeName string) (*goqsan.SnapshotData, error)
	DeleteSnapshot(ctx context.Context, volId, snapId string, permanent bool) error
	GetSnapshot(ctx context.Context, volId, snapId string) (*goqsan.SnapshotData, error)
	GetSnapshotSetting(ctx context.Context, volId string) (*goqsan.SnapshotSetting, error)
	SetSnapshotSetting(ctx context.Context, volId string, options *goqsan.SnapshotMutableSetting) (*goqsan.SnapshotSetting, error)

	GetMetadata(ctx context.Context, volId string) (metastatus, metatype string, metacontent []byte, err error)
	SetMetadata(ctx context.Context, volId, metastatus, metatype string, metacontent []byte) (string, string, []byte, error)
	SetTimestamp(ctx context.Context, volId, timestamp string) (string, error)

	DeleteCloneTask(ctx context.Context, volId string) error
}

// Define a wrapper for FileVolumeOp
type FileVolumeWrapper struct {
	op *goqsan.FileVolumeOp
}

func (w *FileVolumeWrapper) ListVolumeByID(ctx context.Context, volId string) (interface{}, error) {
	return w.op.ListVolumeByID(ctx, volId)
}

func (w *FileVolumeWrapper) ListVolumesByPoolID(ctx context.Context, poolId string) (interface{}, error) {
	return w.op.ListVolumesByPoolID(ctx, poolId)
}

func (w *FileVolumeWrapper) ModifyVolume(ctx context.Context, volId string, opts interface{}) (interface{}, error) {
	fileOpts, ok := opts.(*goqsan.FileVolumeModifyOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options type for FileVolumeOp")
	}
	return w.op.ModifyVolume(ctx, volId, fileOpts)
}

func (w *FileVolumeWrapper) ListSnapshots(ctx context.Context, volId string) (*[]goqsan.SnapshotData, error) {
	return w.op.ListSnapshots(ctx, volId)
}

func (w *FileVolumeWrapper) CreateSnapshot(ctx context.Context, volId, snapName string) (*goqsan.SnapshotData, error) {
	return w.op.CreateSnapshot(ctx, volId, snapName)
}

func (w *FileVolumeWrapper) DeleteSnapshot(ctx context.Context, volId, snapId string, permanent bool) error {
	return w.op.DeleteSnapshot(ctx, volId, snapId, permanent)
}

func (w *FileVolumeWrapper) GetSnapshot(ctx context.Context, volId, snapId string) (*goqsan.SnapshotData, error) {
	return w.op.GetSnapshot(ctx, volId, snapId)
}

func (w *FileVolumeWrapper) GetSnapshotSetting(ctx context.Context, volId string) (*goqsan.SnapshotSetting, error) {
	return w.op.GetSnapshotSetting(ctx, volId)
}

func (w *FileVolumeWrapper) SetSnapshotSetting(ctx context.Context, volId string, options *goqsan.SnapshotMutableSetting) (*goqsan.SnapshotSetting, error) {
	return w.op.SetSnapshotSetting(ctx, volId, options)
}

func (w *FileVolumeWrapper) GetMetadata(ctx context.Context, volId string) (metastatus, metatype string, metacontent []byte, err error) {
	return w.op.GetMetadata(ctx, volId)
}

func (w *FileVolumeWrapper) SetMetadata(ctx context.Context, volId, metastatus, metatype string, metacontent []byte) (string, string, []byte, error) {
	return w.op.SetMetadata(ctx, volId, metastatus, metatype, metacontent)
}

func (w *FileVolumeWrapper) SetTimestamp(ctx context.Context, volId, timestamp string) (string, error) {
	return w.op.SetTimestamp(ctx, volId, timestamp)
}

func (w *FileVolumeWrapper) DeleteCloneTask(ctx context.Context, volId string) error {
	return w.op.DeleteCloneTask(ctx, volId)
}

// Define a wrapper for VolumeOp
type VolumeWrapper struct {
	op *goqsan.VolumeOp
}

func (w *VolumeWrapper) ListVolumeByID(ctx context.Context, volId string) (interface{}, error) {
	return w.op.ListVolumeByID(ctx, volId)
}

func (w *VolumeWrapper) ListVolumesByPoolID(ctx context.Context, poolId string) (interface{}, error) {
	return w.op.ListVolumesByPoolID(ctx, poolId)
}

func (w *VolumeWrapper) ModifyVolume(ctx context.Context, volId string, opts interface{}) (interface{}, error) {
	volumeOpts, ok := opts.(*goqsan.VolumeModifyOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options type for VolumeOp")
	}
	return w.op.ModifyVolume(ctx, volId, volumeOpts)
}

func (w *VolumeWrapper) ListSnapshots(ctx context.Context, volId string) (*[]goqsan.SnapshotData, error) {
	return w.op.ListSnapshots(ctx, volId)
}

func (w *VolumeWrapper) CreateSnapshot(ctx context.Context, volId, snapName string) (*goqsan.SnapshotData, error) {
	return w.op.CreateSnapshot(ctx, volId, snapName)
}

func (w *VolumeWrapper) DeleteSnapshot(ctx context.Context, volId, snapId string, permanent bool) error {
	return w.op.DeleteSnapshot(ctx, volId, snapId, permanent)
}

func (w *VolumeWrapper) GetSnapshot(ctx context.Context, volId, snapId string) (*goqsan.SnapshotData, error) {
	return w.op.GetSnapshot(ctx, volId, snapId)
}

func (w *VolumeWrapper) GetSnapshotSetting(ctx context.Context, volId string) (*goqsan.SnapshotSetting, error) {
	return w.op.GetSnapshotSetting(ctx, volId)
}

func (w *VolumeWrapper) SetSnapshotSetting(ctx context.Context, volId string, options *goqsan.SnapshotMutableSetting) (*goqsan.SnapshotSetting, error) {
	return w.op.SetSnapshotSetting(ctx, volId, options)
}

func (w *VolumeWrapper) GetMetadata(ctx context.Context, volId string) (metastatus, metatype string, metacontent []byte, err error) {
	return w.op.GetMetadata(ctx, volId)
}

func (w *VolumeWrapper) SetMetadata(ctx context.Context, volId, metastatus, metatype string, metacontent []byte) (string, string, []byte, error) {
	return w.op.SetMetadata(ctx, volId, metastatus, metatype, metacontent)
}

func (w *VolumeWrapper) SetTimestamp(ctx context.Context, volId, timestamp string) (string, error) {
	return w.op.SetTimestamp(ctx, volId, timestamp)
}

func (w *VolumeWrapper) DeleteCloneTask(ctx context.Context, volId string) error {
	return w.op.DeleteCloneTask(ctx, volId)
}

func genericListVolumeByID(ctx context.Context, volumeAPI GenericVolumeInterface, volId string) (*genericVolumeData, error) {
	vol, err := volumeAPI.ListVolumeByID(ctx, volId)
	if err != nil {
		return nil, err
	}

	return convertToGenericVolumeData(vol), nil
}

func genericModifyVolume(ctx context.Context, volumeAPI GenericVolumeInterface, volId string, volType *string, resizeVolSizeMB uint64) (*genericVolumeData, error) {
	var opts interface{}

	switch v := volumeAPI.(type) {
	case *VolumeWrapper:
		blockOpts := &goqsan.VolumeModifyOptions{}
		if volType != nil {
			blockOpts.Tags = goqsan.Tag{Type: *volType}
		}
		if resizeVolSizeMB > 0 {
			blockOpts.TotalSize = uint64(resizeVolSizeMB)
		}
		opts = blockOpts
	case *FileVolumeWrapper:
		fileOpts := &goqsan.FileVolumeModifyOptions{}
		if volType != nil {
			fileOpts.Type = *volType
		}
		if resizeVolSizeMB > 0 {
			fileOpts.TotalSize = uint64(resizeVolSizeMB)
		}
		opts = fileOpts
	default:
		return nil, status.Error(codes.Internal, fmt.Sprintf("Unsupported volumeAPI type: %T", v))
	}

	vol, err := volumeAPI.ModifyVolume(ctx, volId, opts)
	if err != nil {
		return nil, err
	}

	return convertToGenericVolumeData(vol), nil
}

func convertToGenericVolumeData(vol interface{}) *genericVolumeData {
	var genricVol genericVolumeData

	switch v := vol.(type) {
	case *goqsan.VolumeData:
		genricVol.ID = v.ID
		genricVol.Name = v.Name
		genricVol.Type = v.Tags.Type
		genricVol.PoolID = v.PoolID
		genricVol.TargetID = v.TargetID
		genricVol.ReplicaTargetID = v.ReplicaTargetID
		genricVol.ReplicaTargetName = v.ReplicaTargetName
		genricVol.LunID = v.LunID
		genricVol.Online = v.Online
		genricVol.State = v.State
		genricVol.Progress = v.Progress
		genricVol.Health = v.Health
		genricVol.Provision = v.Provision
		genricVol.TotalSize = v.TotalSize
		genricVol.UsedSize = v.UsedSize
	case *goqsan.FileVolumeData:
		genricVol.ID = v.ID
		genricVol.Name = v.Name
		genricVol.Type = v.Type
		genricVol.PoolID = v.PoolID
		genricVol.TargetID = ""
		genricVol.ReplicaTargetID = v.ReplicaTargetID
		genricVol.ReplicaTargetName = v.ReplicaTargetName
		genricVol.LunID = ""
		genricVol.Online = v.Online
		genricVol.State = v.State
		genricVol.Progress = v.Progress
		genricVol.Health = v.Health
		genricVol.Provision = v.Provision
		genricVol.TotalSize = v.TotalSize
		genricVol.UsedSize = v.UsedSize
	}
	return &genricVol
}

func getVolumeCondition(vol *genericVolumeData) (abnormal bool, msg string) {
	if vol.Online {
		// Block volume use "Optimal", but File volume use "Good"
		if strings.EqualFold(vol.Health, "OPTIMAL") || strings.EqualFold(vol.Health, "GOOD") {
			if strings.EqualFold(vol.State, "ERASING") {
				return false, fmt.Sprintf("Volume is currently erasing (%d %%)", vol.Progress)
			} else if strings.EqualFold(vol.State, "CLONING") {
				return false, fmt.Sprintf("Volume is currently cloning (%d %%)", vol.Progress)
			} else {
				return false, "Volume is good"
			}
		} else {
			return true, fmt.Sprintf("Volume state is %s with health is %s", vol.State, vol.Health)
		}
	} else {
		return true, "Volume is offline"
	}

	// if vol.Online && strings.EqualFold(vol.Health, "FAILED") {
	// 	return true, "Volume is failed"
	// } else if !vol.Online {
	// 	return true, "Volume is offline"
	// }

	// return false, vol.Health
}

func getPortalIp(portal string) string {
	tokens := strings.Split(portal, ":")
	if len(tokens) == 1 || len(tokens) == 2 {
		return tokens[0]
	} else {
		// Invalid portal format
		return portal
	}
}

func formatIscsiPortal(portal string) string {
	if !strings.Contains(portal, ":") {
		portal += ":" + strconv.Itoa(DefaultIscsiPort)
	}
	return portal
}

func getPoolIDProvByName(ctx context.Context, authClient *goqsan.AuthClient, poolname string) (string, string, bool, error) {
	poolAPI := goqsan.NewPool(authClient)
	pools, err := poolAPI.ListPools(ctx)
	if err != nil {
		return "", "", false, err
	}

	for _, pool := range *pools {
		if pool.Name == poolname {
			return pool.ID, pool.Provision, pool.DedupEnabled, nil
		}
	}

	return "", "", false, fmt.Errorf("[getPoolIDByName] Pool name(%s) is not found", poolname)
}

func getBlockVolumeByName(ctx context.Context, authClient *goqsan.AuthClient, poolId, name string) (*goqsan.VolumeData, error) {
	volumeAPI := goqsan.NewVolume(authClient)
	vols, err := volumeAPI.ListVolumesByPoolID(ctx, poolId)
	if err != nil {
		return nil, err
	}

	for _, vol := range *vols {
		if vol.Name == name {
			return &vol, nil
		}
	}

	return nil, fmt.Errorf("[getBlockVolumeByName] Volume name(%s) is not found", name)
}

func getFileVolumeByName(ctx context.Context, authClient *goqsan.AuthClient, poolId, name string) (*goqsan.FileVolumeData, error) {
	volumeAPI := goqsan.NewFileVolume(authClient)
	vols, err := volumeAPI.ListVolumesByPoolID(ctx, poolId)
	if err != nil {
		return nil, err
	}

	for _, vol := range *vols {
		if vol.Name == name {
			return &vol, nil
		}
	}

	return nil, fmt.Errorf("[getFileVolumeByName] Volume name(%s) is not found", name)
}

func isCSIVolume(ctx context.Context, volumeAPI GenericVolumeInterface, volId string) bool {
	if metastatus, metatype, _, err := volumeAPI.GetMetadata(ctx, volId); err != nil {
		klog.Errorf("[isCSIVolume] Get volId(%s) metadata failed. err: %v", volId, err)
		return false
	} else {
		return validMetaHeader(metastatus, metatype)
	}
}

func allocSnapshotSpace(ctx context.Context, volumeAPI GenericVolumeInterface, volId string) error {
	snapSet, err := volumeAPI.GetSnapshotSetting(ctx, volId)
	if err != nil {
		return fmt.Errorf("Get volume snapshot setting failed: %v", err)
	}
	klog.Infof("[allocSnapshotSpace] Volume snapshot settings: %+v", snapSet)

	/**
	This is a workaround solution for e2e test about XFS filesystem on thin volume.
	*/
	suggestSize := snapSet.SuggestSize
	if suggestSize == 0 {
		suggestSize = snapSet.MinimumSize
	}
	if snapSet.TotalSize < suggestSize {
		fMinSnap := math.Ceil(float64(suggestSize) / 1024)
		if int(fMinSnap) < MIN_SNAPSHOT_SIZE_GB {
			vol, err := genericListVolumeByID(ctx, volumeAPI, volId)
			if err == nil && strings.EqualFold(vol.Provision, ProvisionThin) {
				klog.Infof("[allocSnapshotSpace] Extend volume snapshot size from %dGB to %dGB", int(fMinSnap), MIN_SNAPSHOT_SIZE_GB)
				fMinSnap = MIN_SNAPSHOT_SIZE_GB
				// The below code is unused because the available size of snapshots for thin volumes is excessively large.
				// if (int(fMinSnap) << 10) > snapSet.AvailableSize {
				// 	fMinSnap = float64(snapSet.AvailableSize >> 10)
				// }
			}
		}
		optionsSP := &goqsan.SnapshotMutableSetting{
			TotalSize: int(fMinSnap) << 10,
		}

		retries := 0
	RETRY:
		snapSet, err = volumeAPI.SetSnapshotSetting(ctx, volId, optionsSP)
		if err != nil {
			resterr, ok := err.(*goqsan.RestError)
			if ok && resterr.StatusCode == http.StatusServiceUnavailable && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_TRY_LATER_V2 && retries < 30 {
				klog.Infof("[allocSnapshotSpace] Volume(%s) is not ready to set snapshot space now, sleep 3 sec then try again...", volId)
				time.Sleep(3 * time.Second)
				retries++
				goto RETRY
			} else if ok && resterr.StatusCode == http.StatusConflict && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_TRY_LATER_V1 && retries < 30 {
				klog.Infof("[allocSnapshotSpace] Volume(%s) is not ready to set snapshot space now, sleep 3 sec then try again...", volId)
				time.Sleep(3 * time.Second)
				retries++
				goto RETRY
			} else {
				return fmt.Errorf("Enable snapshot space failed: %v", err)
			}
		}
		klog.Infof("[allocSnapshotSpace] Snapshot space is enabled with size %dMB", snapSet.TotalSize)
	}

	return nil
}

func getSnapshotsByPoolID(ctx context.Context, volumeAPI GenericVolumeInterface, poolId string) (map[string]*[]goqsan.SnapshotData, error) {
	snapCnt := 0
	snapMap := map[string]*[]goqsan.SnapshotData{}
	vols, err := volumeAPI.ListVolumesByPoolID(ctx, poolId)
	if err != nil {
		return nil, fmt.Errorf("[getSnapshotsByPoolID] ListVolumesByPoolID %s failed. err: %v", poolId, err)
	}

	switch vs := vols.(type) {
	case *[]goqsan.VolumeData:
		for _, vol := range *vs {
			if isCSIVolume(ctx, volumeAPI, vol.ID) {
				if ss, _ := volumeAPI.ListSnapshots(ctx, vol.ID); ss != nil {
					var snapData []goqsan.SnapshotData
					for _, s := range *ss {
						if !s.Trash.InTrash {
							snapData = append(snapData, s)
						}
					}
					// klog.Infof("[getSnapshotsByPoolID] volID(%s) volName(%s) allSnapCnt(%d) snapCnt(%d)", vol.ID, vol.Name, len(*ss), len(snapData))
					snapCnt += len(snapData)
					snapMap[vol.ID] = &snapData
				}
			}
		}
	case *[]goqsan.FileVolumeData:
		for _, vol := range *vs {
			if isCSIVolume(ctx, volumeAPI, vol.ID) {
				if ss, _ := volumeAPI.ListSnapshots(ctx, vol.ID); ss != nil {
					var snapData []goqsan.SnapshotData
					for _, s := range *ss {
						if !s.Trash.InTrash {
							snapData = append(snapData, s)
						}
					}
					// klog.Infof("[getSnapshotsByPoolID] volID(%s) volName(%s) allSnapCnt(%d) snapCnt(%d)", vol.ID, vol.Name, len(*ss), len(snapData))
					snapCnt += len(snapData)
					snapMap[vol.ID] = &snapData
				}
			}
		}
	}

	klog.Infof("[getSnapshotsByPoolID] poolId(%s) snapCnt: %d", poolId, snapCnt)
	return snapMap, nil
}

func collectSnapshotsForVolumes[T any](ctx context.Context, d *Driver, volumeAPI GenericVolumeInterface, ip string,
	vols *[]T,
	getID func(T) string,
	getPoolID func(T) string,
) (map[string]*[]goqsan.SnapshotData, int) {
	snapCnt := 0
	snapMap := make(map[string]*[]goqsan.SnapshotData)

	for _, vol := range *vols {
		volID := getID(vol)
		if metastatus, metatype, metacontent, err := volumeAPI.GetMetadata(ctx, volID); err != nil {
			klog.Errorf("[collectSnapshotsForVolumes] Get volId(%s) metadata failed. err: %v", volID, err)
			continue
		} else if validMetaHeader(metastatus, metatype) {
			if ss, _ := volumeAPI.ListSnapshots(ctx, volID); ss != nil {
				var snapData []goqsan.SnapshotData
				for _, s := range *ss {
					if !s.Trash.InTrash {
						snapData = append(snapData, s)
					}
				}
				snapCnt += len(snapData)

				var volumeContextID string
				if getProtocolFromMetaContent(metacontent) == protocolNFS {
					if fw, ok := volumeAPI.(*FileVolumeWrapper); ok {
						fileVolumeAPI := fw.op
						var shareId, targetId string
						share, err := getShareFromFileVolume(ctx, fileVolumeAPI, volID)
						if err != nil {
							klog.Warningf("[collectSnapshotsForVolumes] getShareFromFileVolume(%s) failed. err: %v", volID, err)
						} else {
							shareId = share.ID
						}

						// TODO, get targetId

						volumeContextID = d.GenerateVolumeContextID(getProtocolFromMetaContent(metacontent), ip, getPoolID(vol), volID, &shareId, &targetId)
					} else {
						klog.Errorf("[collectSnapshotsForVolumes] Failed to convert GenericVolumeInterface to FileVolumeWrapper")

						shareId := ""
						targetId := ""
						volumeContextID = d.GenerateVolumeContextID(getProtocolFromMetaContent(metacontent), ip, getPoolID(vol), volID, &shareId, &targetId)
					}
				} else {
					volumeContextID = d.GenerateVolumeContextID(getProtocolFromMetaContent(metacontent), ip, getPoolID(vol), volID, nil, nil)
				}
				snapMap[volumeContextID] = &snapData
			}
		}
	}

	return snapMap, snapCnt
}

func mergeSnapMaps(dst, src map[string]*[]goqsan.SnapshotData) {
	for k, v := range src {
		dst[k] = v
	}
}

func getAllSnapshots(ctx context.Context, d *Driver) (map[string]*[]goqsan.SnapshotData, error) {
	totalSnapCnt := 0
	snapMap := map[string]*[]goqsan.SnapshotData{}
	for ip, authClient := range d.qsan.authClient {
		snapCnt, fsnapCnt := 0, 0

		volumeAPI := goqsan.NewVolume(authClient)
		if vols, _ := volumeAPI.ListVolumes(ctx); vols != nil {
			volSnapMap, cnt := collectSnapshotsForVolumes(
				ctx, d, &VolumeWrapper{op: volumeAPI}, ip, vols,
				func(v goqsan.VolumeData) string { return v.ID },
				func(v goqsan.VolumeData) string { return v.PoolID },
			)

			snapCnt = cnt
			mergeSnapMaps(snapMap, volSnapMap)
		}

		fvolumeAPI := goqsan.NewFileVolume(authClient)
		if fvols, _ := fvolumeAPI.ListVolumes(ctx); fvols != nil {
			volSnapMap, cnt := collectSnapshotsForVolumes(
				ctx, d, &FileVolumeWrapper{op: fvolumeAPI}, ip, fvols,
				func(v goqsan.FileVolumeData) string { return v.ID },
				func(v goqsan.FileVolumeData) string { return v.PoolID },
			)

			fsnapCnt = cnt
			mergeSnapMaps(snapMap, volSnapMap)
		}

		klog.Infof("[getAllSnapshots] Server(%s) snapCnt(%d) fsnapCnt(%d)", ip, snapCnt, fsnapCnt)
		totalSnapCnt += (snapCnt + fsnapCnt)
	}

	klog.Infof("[getAllSnapshots] totalSnapCnt: %d", totalSnapCnt)
	return snapMap, nil
}

func cleanSnapshotInTrash(ctx context.Context, volumeAPI GenericVolumeInterface, volId string) (int, int) {
	var cleanCnt, trashCnt int = 0, 0
	ss, _ := volumeAPI.ListSnapshots(ctx, volId)
	if ss != nil {
		for idx, s := range *ss {
			if s.Trash.InTrash {
				klog.Infof("[cleanSnapshotInTrash] Trash snapshot(%d): %+v", idx, s)
				trashCnt++
			}
		}

		for idx, s := range *ss {
			klog.Infof("[cleanSnapshotInTrash] snapshot(%d): %+v", idx, s)
			if s.Trash.InTrash {
				klog.Infof("[cleanSnapshotInTrash] Delete snapshot %s", s.ID)
				if err := volumeAPI.DeleteSnapshot(ctx, volId, s.ID, true); err != nil {
					klog.Warningf("[cleanSnapshotInTrash] Failed to delete snapshot(%s, %s) snapCnt(%d), err: %v", volId, s.ID, err)
				} else {
					cleanCnt++
				}
			} else {
				break
			}
		}
	}
	klog.Infof("[cleanSnapshotInTrash] volId(%s) snapCnt(%d) cleanCnt(%d) trashCnt(%d)", volId, len(*ss), cleanCnt, trashCnt)

	return cleanCnt, trashCnt
}

func getProtocolFromMetaContent(metacontent []byte) string {
	t := metacontent[0] & 0xf
	if t == META_KIND_ISCSI {
		return protocolISCSI
	} else if t == META_KIND_FC {
		return protocolFC
	} else if t == META_KIND_NFS {
		return protocolNFS
	} else {
		klog.Errorf("Unknown kind(%v) of meta content: %v", t, metacontent)
		return protocolISCSI
	}
}

func getTargetIDByIqn(ctx context.Context, authClient *goqsan.AuthClient, iscsiTargets string) (string, error) {
	tgtID := ""
	targetAPI := goqsan.NewTarget(authClient)
	targetArr := strings.Split(iscsiTargets, ",")
	for i := range targetArr {
		targetArr[i] = strings.TrimSpace(targetArr[i])
	}

	tgts, err := targetAPI.ListTargets(ctx, "")
	if err != nil {
		return "", err
	}

	for i := range targetArr {
		for _, tgt := range *tgts {
			// fmt.Printf("\n\n[listTarget] tgt = %+v", tgt.Iscsi[0].Name)
			if contains(iscsiTargetTypes, tgt.Type) {
				for _, iscsi := range tgt.Iscsi {
					// fmt.Printf("\n\n[listTarget] tgt(%s) = %+v", tgt.ID, iscsi.Iqn)
					// klog.Infof("[ControllerPublishVolume] %s vs %s, tgtID=%s", targetArr[i], iscsi.Iqn, tgt.ID)
					if targetArr[i] == iscsi.Iqn {
						tgtID = tgt.ID
						// klog.Infof("[ControllerPublishVolume] FOUND tgtID: %s", tgtID)
						// break
						return tgtID, nil
					}
				}
			}
		}
	}

	return "", fmt.Errorf("Target %v is not found", iscsiTargets)
}

func getTargetIDByName(ctx context.Context, authClient *goqsan.AuthClient, ttypes []string, tname string) (string, error) {
	targetAPI := goqsan.NewTarget(authClient)
	tgts, err := targetAPI.ListTargets(ctx, "")
	if err != nil {
		return "", err
	}

	for _, tgt := range *tgts {
		if contains(ttypes, tgt.Type) && tgt.Name == tname {
			return tgt.ID, nil
		}
	}

	return "", fmt.Errorf("[getTargetIDByName] Target name(%s) is not found", tname)
}

func getIscsiTargetsByTName(ctx context.Context, authClient *goqsan.AuthClient, tname string) ([]string, error) {
	targetAPI := goqsan.NewTarget(authClient)
	tgts, err := targetAPI.ListTargets(ctx, "")
	if err != nil {
		return nil, err
	}

	var targetArr []string
	for _, tgt := range *tgts {
		if contains(iscsiTargetTypes, tgt.Type) && tgt.Name == tname {
			for _, iscsi := range tgt.Iscsi {
				targetArr = append(targetArr, iscsi.Iqn)
			}
		}
	}

	return targetArr, nil
}

// mapIscsiTargetToEthIds maps the iSCSI target(host group) to its associated Ethernet IDs.
// It returns a map that maps each portal to its corresponding target,
// and an error if any occurred during the mapping process.
//
// For example,
//
//	  m := mapIscsiTargetToEthIds(ctx, authClient, "HostGroup_Johnson")
//	  // Map example:
//	  // m["iqn.2004-08.com.qsan:xs5312-000d5b3a0:dev2.ctr1"] = [c0e1, c0e2, c0e3, c0e4]
//		 // m["iqn.2004-08.com.qsan:xs5312-000d5b3a0:dev2.ctr2"] = [c1e1, c1e2, c1e3, c1e4]
func mapIscsiTargetToEthIds(ctx context.Context, authClient *goqsan.AuthClient, tname string) (map[string][]string, error) {
	targetAPI := goqsan.NewTarget(authClient)
	tgts, err := targetAPI.ListTargets(ctx, "")
	if err != nil {
		return nil, err
	}

	m := make(map[string][]string)
	for _, tgt := range *tgts {
		if contains(iscsiTargetTypes, tgt.Type) && tgt.Name == tname {
			for _, iscsi := range tgt.Iscsi {
				m[iscsi.Iqn] = iscsi.Eths
			}
		}
	}

	return m, nil
}

func getIscsiPortalsByTName(ctx context.Context, authClient *goqsan.AuthClient, tname string) ([]string, error) {
	targetToEthIdsMap, err := mapIscsiTargetToEthIds(ctx, authClient, tname)
	if err != nil {
		return nil, fmt.Errorf("[getIscsiPortalsByTName] mapIscsiTargetToEthIds failed: %v", err)
	}

	var ethIdArr []string
	for _, ethIds := range targetToEthIdsMap {
		ethIdArr = append(ethIdArr, ethIds...)
	}

	networkAPI := goqsan.NewNetwork(authClient)
	nics, err := networkAPI.ListNICs(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("[getIscsiPortalsByTName] ListNICs failed: %v", err)
	}

	var portalArr []string
	for _, ethId := range ethIdArr {
		for _, nic := range *nics {
			if ethId == nic.ID {
				portalArr = append(portalArr, nic.Ipv4.IP)
			}
		}
	}

	return portalArr, nil
}

// mapIscsiPortalsToTargets maps the iSCSI portals to their corresponding targets.
// It returns a map that maps each portal to its corresponding target,
// and an error if any occurred during the mapping process.
//
// For example,
//
//	  m := mapIscsiPortalsToTargets(ctx, authClient, "HostGroup_Johnson", []string{"192.168.201.201", "192.168.201.202:3260", "192.168.201.200"})
//	  // Map example:
//	  // m["192.168.201.201"] = "iqn.2004-08.com.qsan:xs5312-000d5b3a0:dev2.ctr1"
//		 // m["192.168.201.202:3260"] = "iqn.2004-08.com.qsan:xs5312-000d5b3a0:dev2.ctr1"
//		 // m["192.168.201.200"] = "iqn.2004-08.com.qsan:xs5312-000d5b3a0:dev2.ctr2"
func mapIscsiPortalsToTargets(ctx context.Context, authClient *goqsan.AuthClient, tname string, portals []string) (map[string]string, error) {
	targetToEthIdsMap, err := mapIscsiTargetToEthIds(ctx, authClient, tname)
	if err != nil {
		return nil, fmt.Errorf("[mapIscsiPortalsToTargets] mapIscsiTargetToEthIds failed: %v", err)
	}

	networkAPI := goqsan.NewNetwork(authClient)
	nics, err := networkAPI.ListNICs(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("[mapIscsiPortalsToTargets] ListNICs failed: %v", err)
	}

	m := make(map[string]string)
	for _, portal := range portals {
		portalIp := getPortalIp(portal)
		for _, nic := range *nics {
			if portalIp == nic.Ipv4.IP {
				// nic.ID
				for iscsiTarget, ethIds := range targetToEthIdsMap {
					if contains(ethIds, nic.ID) {
						m[portal] = iscsiTarget
					}
				}
			}
		}
	}

	return m, nil
}

func getFcWwnByTName(ctx context.Context, authClient *goqsan.AuthClient, tname string) (string, error) {
	targetAPI := goqsan.NewTarget(authClient)
	tgts, err := targetAPI.ListTargets(ctx, "")
	if err != nil {
		return "", err
	}

	for _, tgt := range *tgts {
		if contains(fcTargetTypes, tgt.Type) && tgt.Name == tname {
			for _, fc := range tgt.Fcp {
				return strings.ToLower(fc.Wwn), nil
			}
		}
	}

	return "", fmt.Errorf("[getFcWwnByTName] Target name(%s) is not found", tname)
}

func getLunDataByVolumeId(ctx context.Context, authClient *goqsan.AuthClient, volId string) (*goqsan.LunData, error) {
	volumeAPI := goqsan.NewVolume(authClient)
	targetAPI := goqsan.NewTarget(authClient)
	vol, err := volumeAPI.ListVolumeByID(ctx, volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("getLunDataByVolumeId, Volume ID %s not found.", volId)
		}
		return nil, err
	}

	lunData, err := targetAPI.ListTargetLun(ctx, vol.TargetID, vol.LunID)
	if err != nil {
		return nil, err
	}
	// klog.Infof("[NodePublishVolume] lunData: %+v", lunData)
	return lunData, nil
}

// checkAndEraseDevice erases thin volume at first time to avoid a dirty filesystem,
// then clear NEP bit if erasing successful
func checkAndEraseDevice(ctx context.Context, authClient *goqsan.AuthClient, volId, devPath, fsType string) (bool, error) {
	volumeAPI := goqsan.NewVolume(authClient)

	// To prevent cancel from kubernetes, create a new context.
	restCtx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()

	var erased bool = false
	metastatus, metatype, metacontent, err := volumeAPI.GetMetadata(restCtx, volId)
	if err == nil && validMetaHeader(metastatus, metatype) {
		klog.Infof("[checkAndEraseDevice] volId(%s) metadata %v", volId, metacontent)
		b := metacontent[MCIDX_ATTR]
		if b&META_ATTR_NEP == META_ATTR_NEP {
			if err := eraseDevice(devPath, fsType); err != nil {
				return erased, err
			}

			metacontent[MCIDX_ATTR] = metacontent[MCIDX_ATTR] &^ META_ATTR_NEP
			klog.Infof("[checkAndEraseDevice] Set volId(%s) metadata %v", volId, metacontent)
			_, _, _, err = volumeAPI.SetMetadata(restCtx, volId, metastatus, metatype, metacontent)
			if err != nil {
				return erased, fmt.Errorf("[checkAndEraseDevice] Set volId(%s) metadata failed: %+v", volId, err)
			}

			erased = true
		}
	}

	return erased, nil
}

func eraseDevice(devPath, fsType string) error {
	_, err := execCmd("wipefs", []string{"-a", "-f", devPath}...)
	if err != nil {
		_, err := os.Stat(devPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("[eraseDevice] device(%s) does not exist", devPath)
		}

		klog.Warningf("[eraseDevice] BINGO!!! Use dd %s: %v", devPath, err)
		_, err = execCmd("dd", []string{"if=/dev/zero", "of=" + devPath, "bs=1M", "count=10", "conv=fsync"}...)
		if err != nil {
			return fmt.Errorf("Erase device(%s) first 10MB failed: %v", devPath, err)
		}
	} else {
		if fsType == "ext3" {
			// Also use dd first 10MB for ext3 to fix a e2e test item,
			// [Testpattern: Dynamic PV (ext3)] volumes [It] should store data

			// klog.Infof("[eraseDevice] Also use dd %s for %s", devPath, fsType)
			_, err = execCmd("dd", []string{"if=/dev/zero", "of=" + devPath, "bs=1M", "count=10", "conv=fsync"}...)
			if err != nil {
				return fmt.Errorf("Erase device(%s) first 10MB failed: %v", devPath, err)
			}

		}
	}

	return nil
}

func repairDevice(fsType, devPath string) error {
	var out string
	var err error

	if fsType == "ext3" || fsType == "ext4" {
		out, err = execCmd("e2fsck", []string{"-fp", devPath}...)
		if err != nil {
			return fmt.Errorf("e2fsck device(%s) failed: %v", devPath, err)
		}
	} else if fsType == "xfs" {
		out, err = execCmd("xfs_repair", []string{"-n", devPath}...)
		if err != nil {
			return fmt.Errorf("xfs_repair device(%s) failed: %v", devPath, err)
		}
	}

	klog.V(2).Infof("[repairDevice] fsType(%s) devPath(%s): %+v", fsType, devPath, out)
	return nil
}

func getDeviceNameFromBlockDevice(devicePath string) (string, error) {
	args := []string{"-rn", "-o", "KNAME"}
	out, err := execCmd("lsblk", append(args, []string{devicePath}...)...)
	if err != nil {
		return "", err
	}

	kname := strings.Trim(string(out), "\n")
	if len(kname) > 0 {
		devName := "/dev/" + kname
		return devName, nil
	} else {
		return "", fmt.Errorf("KNAME is empty! devicePath(%s)\n", devicePath)
	}
}

func GetDeviceNameFromTargetPath(targetPath string) (string, error) {
	fi, err := os.Lstat(targetPath)
	if err != nil {
		return "", fmt.Errorf("GetDeviceNameFromTargetPath(targetPath=%s) Lstat failed. err: %v", targetPath, err)
	}
	if fi.Mode()&fs.ModeDir != 0 {
		// devName, refCnt, err := mount.GetDeviceNameFromMount(ns.mounter, stagingPath)
		// if err != nil {
		// 	// klog.Warningf("[NodeUnstageVolume] GetDeviceNameFromMount failed: %v", err)
		// 	return nil, status.Error(codes.Internal, err.Error())
		// }
	} else if fi.Mode()&fs.ModeDevice != 0 {
		return getDeviceNameFromBlockDevice(targetPath)
	}

	return "", fmt.Errorf("devicePath type is neither Dir nor Device\n")
}

func GetFcTargetNodeNames() []string {
	nodeMap := make(map[string]bool)
	nodeNames := []string{}

	fc := &gofc.FCUtil{}
	ports := fc.GetTargetPorts()
	klog.V(4).Infof("[GetFcTargetNodeNames] %+v", ports)
	for _, port := range ports {
		nodeName := strings.TrimPrefix(port.NodeName, "0x")
		if !nodeMap[nodeName] {
			nodeMap[nodeName] = true
			nodeNames = append(nodeNames, nodeName)
		}
	}

	return nodeNames
}

func generateMetadata(protocol, ipv4, driverVersion, buildDate string) (metastatus, metatype string, metacontent []byte) {
	b1 := make([]byte, 1)
	b1[0] = META_VER << 4
	if protocol == protocolISCSI {
		b1[0] = b1[0] | META_KIND_ISCSI
	} else if protocol == protocolFC {
		b1[0] = b1[0] | META_KIND_FC
	} else if protocol == protocolNFS {
		b1[0] = b1[0] | META_KIND_NFS
	}
	ipHex := ipv4ToByte(ipv4)
	b2 := []byte(ipHex)
	metacontent = append(b1, b2...)
	b7 := driverVersionToByte(driverVersion, buildDate)
	metacontent = append(metacontent[:6], b7...)

	return "VALID", "CSI Driver", metacontent
}

// Here are examples of string format:
//
//	v1.0.0-dev (build 2023-03-03T09:29:52Z)
//	v1.0.0-2 (build 2023-03-03T09:29:52Z)
func driverVersionToByte(version, buildDate string) []byte {
	var major, minor, rev, build int
	mver := make([]byte, 4)

	fmt.Sscanf(version, "v%d.%d.%d-%d", &major, &minor, &rev, &build)
	mver[0] = byte((major << 5) | (minor << 2) | (rev >> 2))
	mver[1] = byte((rev&0x3)<<6 | build)

	tt, err := time.Parse(time.RFC3339, buildDate)
	if err != nil {
		klog.Warningf("[driverVersionToByte] buildDate(%d) parse failed. err: %v", buildDate, err)
		return mver
	}

	klog.Infof("[driverVersionToByte] Major(%d) Minor(%d) Rev(%d) Build(%d) Year(%d) Month(%d)  Day(%d)", major, minor, rev, build, tt.Year(), tt.Month(), tt.Day())

	mver[2] = byte((tt.Year()-2000)<<1 | int(tt.Month())>>3)
	mver[3] = byte((int(tt.Month())&0x7)<<5 | tt.Day())
	return mver
}

func validMetaHeader(metastatus, metatype string) bool {
	if strings.EqualFold(metastatus, "VALID") && strings.EqualFold(metatype, "CSI Driver") {
		return true
	} else {
		return false
	}
}

func hasBindMnt(diskName string) bool {
	cmd := fmt.Sprintf("findmnt -t devtmpfs | grep %s | wc -l", diskName)
	out, err := exec.Command("bash", "-c", cmd).Output()
	klog.Infof("hasBindMnt, bind mount cnt: %s\n", string(out))
	if err != nil {
		// return "", fmt.Errorf("%s (%s)\n", strings.TrimRight(string(out), "\n"), err)
		klog.Warningf("hasBindMnt failed. err: %v\n", err)
		return false
	}

	cnt, _ := strconv.ParseUint(strings.TrimRight(string(out), "\n"), 10, 32)
	if cnt > 0 {
		return true
	} else {
		return false
	}
}

func getBindMounts(devPath string) ([]string, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc/self/mountinfo: %v", err)
	}
	defer file.Close()

	var mountPoints []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Split the line by " - " to separate mount info and device info
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			continue // Invalid line, skip it
		}

		deviceInfo := strings.Fields(parts[0])
		if len(deviceInfo) < 4 {
			continue // Skip malformed lines
		}

		if filepath.Base(deviceInfo[3]) == devPath {
			mountInfo := strings.Fields(parts[0])
			if len(mountInfo) > 4 {
				mountPoints = append(mountPoints, mountInfo[4])
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading /proc/self/mountinfo: %v", err)
	}

	return mountPoints, nil
}

func getAdminUserId(ctx context.Context, authClient *goqsan.AuthClient) (string, error) {
	accountAPI := goqsan.NewAccount(authClient)
	users, err := accountAPI.ListUsers(ctx)
	if err != nil {
		return "", fmt.Errorf("ListUsers failed: %v", err)
	}
	for _, user := range *users {
		if user.Name == "admin" {
			return user.ID, nil
		}
	}

	return "", fmt.Errorf("admin user is not found.")
}

func getAdminGroupId(ctx context.Context, authClient *goqsan.AuthClient) (string, error) {
	accountAPI := goqsan.NewAccount(authClient)
	groups, err := accountAPI.ListGroups(ctx)
	if err != nil {
		return "", fmt.Errorf("ListUsers failed: %v", err)
	}
	for _, group := range *groups {
		if group.Name == "Administrator_Group" {
			return group.ID, nil
		}
	}

	return "", fmt.Errorf("Administrator_Group group is not found.")
}

func addShareToSharehost(ctx context.Context, authClient *goqsan.AuthClient, sharehostId, shareId string) (added bool, err error) {
	klog.Infof("[addShareToSharehost] ShareId(%s) sharehostId(%s)", shareId, sharehostId)
	sharehostAPI := goqsan.NewSharehost(authClient)
	sharehost, err := sharehostAPI.ListSharehostByID(ctx, sharehostId)
	if err != nil {
		return false, fmt.Errorf("[addShareToSharehost] Failed to ListSharehostByID(%s). err: %v", sharehostId, err)
	}

	for _, share := range sharehost.ConnectedShare {
		if shareId == share.ID {
			// Found. Already exists
			klog.Infof("[addShareToSharehost] ShareId(%s) was already added into sharehost(%s)", shareId, sharehostId)
			return false, nil
		}
	}

	start := time.Now()
	if err := sharehostAPI.AddSharehostShares(ctx, sharehostId, []string{shareId}); err != nil {
		return false, status.Errorf(codes.Internal, "Failed to add share(%s) to sharehost(%s). err: %v", shareId, sharehostId, err)
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Second {
		klog.Warningf("[addShareToSharehost] Add share(%s) to sharehost(%s) elapsed: %s", shareId, sharehostId, elapsed)
	}

	return true, nil
}

func removeShareFromSharehost(ctx context.Context, authClient *goqsan.AuthClient, sharehostId, shareId string) error {
	klog.Infof("[removeShareFromSharehost] ShareId(%s) sharehostId(%s)", shareId, sharehostId)
	sharehostAPI := goqsan.NewSharehost(authClient)

	start := time.Now()
	if err := sharehostAPI.RemoveSharehostShares(ctx, sharehostId, []string{shareId}); err != nil {
		return status.Errorf(codes.Internal, "Failed to remove share(%s) to sharehost(%s). err: %v", shareId, sharehostId, err)
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Second {
		klog.Warningf("[removeShareFromSharehost] Remove share(%s) to sharehost(%s) elapsed: %s", shareId, sharehostId, elapsed)
	}

	return nil
}

func getShareFromFileVolume(ctx context.Context, volumeAPI *goqsan.FileVolumeOp, volId string) (*goqsan.ShareData, error) {
	shares, err := volumeAPI.ListShareByVolumeID(ctx, volId)
	if err != nil {
		return nil, err
	}

	if len(*shares) != 1 {
		klog.Warningf("[getShareFromFileVolume] The share count of volume(%s) is not equal to 1. (cnt=%d)", volId, len(*shares))
	}

	if len(*shares) >= 1 {
		return &(*shares)[0], nil
	} else {
		return nil, fmt.Errorf("No shares found in volume(%s).", volId)
	}
}

func IsServerSupportRDMA(ctx context.Context, authClient *goqsan.AuthClient, ip string) bool {
	networkAPI := goqsan.NewNetwork(authClient)
	if nics, err := networkAPI.ListNICs(ctx, ""); err != nil {
		klog.Warningf("[IsServerSupportRDMA] ListNICs failed, err: %v", err)
		return false
	} else {
		for _, nic := range *nics {
			if nic.Ipv4.IP == ip {
				return nic.RdmaSupport
			}
		}

		// If not found in NICs
		clusters, err := networkAPI.ListClusters(ctx)
		if err != nil {
			klog.Warningf("[IsServerSupportRDMA] ListClusters failed: %v", err)
			return false
		} else {
			for _, clusterIp := range clusters.ClusterIps {
				if clusterIp.IP == ip {
					for _, nic := range *nics {
						if clusterIp.Interface == nic.Interface {
							return nic.RdmaSupport
						}
					}
				}
			}
		}
	}

	return false
}
