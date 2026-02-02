package driver

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"gitlab.qsan.com/sharedlibs-go/gofc"
	"gitlab.qsan.com/sharedlibs-go/goiscsi"
	"gitlab.qsan.com/sharedlibs-go/goqsan"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

// NodeServer driver
type NodeServer struct {
	Driver  *Driver
	mutex   sync.Mutex
	mounter *mount.SafeFormatAndMount
}

// NodeStageVolume stage volume
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	if req.VolumeContext["protocol"] == protocolNFS {
		return &csi.NodeStageVolumeResponse{}, nil
	} else {
		if volCap.GetBlock() != nil {
			return &csi.NodeStageVolumeResponse{}, nil
		} else if volCap.GetMount() != nil {
			return ns.stageBlockVolumeFilesystem(ctx, req)
		}

		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume, Unknown volume type!")
	}
}

func (ns *NodeServer) stageBlockVolumeFilesystem(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	stagingPath := req.GetStagingTargetPath()
	if len(stagingPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging path not provided")
	}

	accessMode := req.GetVolumeCapability().GetAccessMode()
	if accessMode == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume access mode missing in request")
	}

	volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[stageBlockVolumeFilesystem] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	authClient, err := ns.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	ctx = context.Background()
	diskName, err := ns.getVolumeDiskName(ctx, authClient, req.GetPublishContext(), req.GetVolumeContext(), req.GetSecrets())
	if err != nil {
		return nil, err
	}

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(stagingPath)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("[stageBlockVolumeFilesystem] MkdirAll stagingPath(%s)", stagingPath)
			if err = os.MkdirAll(stagingPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("stageBlockVolumeFilesystem, create staging path(%s): %v", stagingPath, err))
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, fmt.Sprintf("stageBlockVolumeFilesystem, check staging path(%s): %v", stagingPath, err))
		}
	}

	klog.Infof("[stageBlockVolumeFilesystem] volumeID(%s) stagingPath(%s) notMnt(%v) secrets(%+v)", volumeID, stagingPath, notMnt, req.GetSecrets())
	if !notMnt {
		// It's already mounted.
		klog.Infof("[stageBlockVolumeFilesystem] stagingPath %s: already mounted", stagingPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	devPath := "/dev/" + diskName
	mountOptions := req.GetVolumeCapability().GetMount().GetMountFlags()
	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	if !contains(supportFsTypes, fsType) {
		klog.Infof("[stageBlockVolumeFilesystem] fsType(%s) Not supported. Use default type: %s\n", fsType, DefaultFsType)
		fsType = DefaultFsType
	}

	existingFormat, err := ns.mounter.GetDiskFormat(devPath)
	if err != nil {
		klog.Errorf("[stageBlockVolumeFilesystem] failed to get disk format for path %s, error: %v", devPath, err)
	}
	klog.Infof("[stageBlockVolumeFilesystem] devPath(%s) existingFormat(%s)", devPath, existingFormat)

	if accessMode.GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		earsed, err := checkAndEraseDevice(ctx, authClient, volData.volId, devPath, fsType)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("checkAndEraseDevice(%s, %s) failed: %v", volData.volId, devPath, err))
		}

		if earsed {
			// Check if the filesystem type has been completely cleared.
			existingFormat, err := ns.mounter.GetDiskFormat(devPath)
			if err != nil {
				klog.Errorf("[stageBlockVolumeFilesystem] 22 failed to get disk format for path %s, error: %v", devPath, err)
			}
			if len(existingFormat) > 0 {
				klog.Errorf("[stageBlockVolumeFilesystem] 22 devPath(%s) existingFormat(%s) NOT CLEAR !!", devPath, existingFormat)
			}
		} else {
			if len(existingFormat) > 0 {
				if existingFormat != fsType {
					klog.Warningf("[stageBlockVolumeFilesystem] Disk fsType mismatch (%s vs %s)! Use existing fsType: %s", existingFormat, fsType, existingFormat)
					fsType = existingFormat
				}
			} else {
				if req.GetPublishContext()["pvolId"] != "" {
					klog.Warningf("[stageBlockVolumeFilesystem] Backup disk fsType is empty! (pvolId: %s)", req.GetPublishContext()["pvolId"])
				}
			}
		}

		options := []string{"rw"}
		if fsType == "xfs" {
			options = append(options, "nouuid")
		}
		options = append(options, mountOptions...)
		klog.Infof("[stageBlockVolumeFilesystem] FormatAndMount volumeID(%v) devPath(%s) stagingPath(%s) fsType(%v) options(%v)", volumeID, devPath, stagingPath, fsType, options)
		if err = ns.mounter.FormatAndMount(devPath, stagingPath, fsType, options); err != nil {
			klog.Warningf("[stageBlockVolumeFilesystem] FormatAndMount volumeID(%v) failed. err: %v \ntry to recover it", volumeID, err)

			if err := ns.mounter.Unmount(stagingPath); err != nil {
				klog.Warningf("[stageBlockVolumeFilesystem] Unmount %s failed. err: %v", stagingPath, err)
			}
			// if err := repairDevice(fsType, devPath); err != nil {
			// 	klog.Warningf("[stageBlockVolumeFilesystem] repairDevice(%s) failed. err: %v", devPath, err)
			// }
			if volData.protocol == protocolISCSI {
				iscsi := &goiscsi.ISCSIUtil{Opts: goiscsi.ISCSIOptions{Timeout: DefaultIscsiTimeout}}
				if err := iscsi.RemoveDisk(devPath); err != nil {
					klog.Errorf("[stageBlockVolumeFilesystem] Remove iSCSI device %s failed. err: %v", devPath, err)
				}
			} else if volData.protocol == protocolFC {
				fc := &gofc.FCUtil{}
				if err := fc.RemoveDisk(devPath); err != nil {
					klog.Errorf("[stageBlockVolumeFilesystem] Remove FC device %s failed. err: %v", devPath, err)
				}
			}

			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount volume %s [%s] to %s, error %v", devPath, fsType, stagingPath, err))
		}

		resizeVolSizeMB, _ := strconv.ParseUint(req.GetVolumeContext()[paramResize], 10, 64)
		if resizeVolSizeMB > 0 {
			if err := ns.ResizeDeviceFilesystem(devPath, stagingPath); err != nil {
				return nil, status.Errorf(codes.Internal, "[stageBlockVolumeFilesystem] ResizeDeviceFilesystem(%s, %s) failed: %v", devPath, stagingPath, err)
			}
		}

		devName, refCnt, err := mount.GetDeviceNameFromMount(ns.mounter, stagingPath)
		if err != nil {
			klog.Warningf("[stageBlockVolumeFilesystem] GetDeviceNameFromMount failed: %v", err)
		}
		klog.Infof("[stageBlockVolumeFilesystem] GetDeviceNameFromMount, devName(%s) refCnt(%d)", devName, refCnt)

	} else {
		if len(existingFormat) == 0 {
			return nil, status.Error(codes.FailedPrecondition, "stageBlockVolumeFilesystem, no filesystem exists on the disk!")
		}
		if existingFormat != fsType {
			return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("stageBlockVolumeFilesystem, Disk fsType mismatch (%s vs %s)!", existingFormat, fsType))
		}
		if !contains(supportFsTypes, existingFormat) {
			return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("stageBlockVolumeFilesystem, Disk fsType(%s) Not supported.", existingFormat))
		}

		options := []string{"ro"}
		options = append(options, mountOptions...)
		klog.Infof("[stageBlockVolumeFilesystem] Mount volumeID(%v) devPath(%s) stagingPath(%s) fsType(%v) options(%v)", volumeID, devPath, stagingPath, fsType, options)
		if err := ns.mounter.Mount(devPath, stagingPath, "", options); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount devPath: %s to %s: %v", devPath, stagingPath, err))
		}
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) getVolumeDiskName(ctx context.Context, authClient *goqsan.AuthClient, publishContext, volumeContext, secrets map[string]string) (string, error) {
	protocol, iscsiPortals, iscsiTargets, _, targetName, parentVolumeID, _, multipathEnabled, resizeVolSizeMB := parseVolumeContext(volumeContext)
	lun, _ := strconv.ParseUint(publishContext["lunNum"], 10, 32)
	klog.Infof("[getVolumeDiskName] lunId(%s) lunNum(%d) protocol(%s) iscsiPortals(%s) iscsiTargets(%s) targetName(%s) parentVolumeID(%s) multipathEnabled(%v) resizeVolSizeMB(%d)",
		publishContext["lunId"], lun, protocol, iscsiPortals, iscsiTargets, targetName, parentVolumeID, multipathEnabled, resizeVolSizeMB)

	var diskName string
	if protocol == protocolISCSI {
		chapUser := strings.TrimSpace(secrets["username"])
		chapPasswd := strings.TrimSpace(secrets["password"])

		var targetMap map[string]string
		var err error
		if multipathEnabled {
			portalArr := splitAndTrimSpace(iscsiPortals, ",")
			targetMap, err = mapIscsiPortalsToTargets(ctx, authClient, targetName, portalArr)
			if err != nil {
				return "", status.Error(codes.InvalidArgument, fmt.Sprintf("mapIscsiPortalsToTargets(%s, %v) failed: %v", targetName, portalArr, err))
			}
			klog.Infof("[getVolumeDiskName] targetName(%s) %+v, secrets(%+v)", targetName, targetMap, secrets)
			if len(targetMap) == 0 {
				return "", status.Error(codes.Internal, fmt.Sprintf("Cant not get iSCSI portals of target(%s)", targetName))
			}

		} else {
			targetMap = map[string]string{
				formatIscsiPortal(iscsiPortals): iscsiTargets,
			}
			klog.Infof("[getVolumeDiskName] targetMap(%+v) secrets(%+v)", targetMap, secrets)
		}

		if diskName, err = ns.getIscsiDiskName(targetMap, lun, chapUser, chapPasswd, multipathEnabled); err != nil {
			klog.Errorf("[getVolumeDiskName] iSCSI disk(%s) is not ready", diskName)
			return "", status.Error(codes.FailedPrecondition, fmt.Sprintf("getIscsiDiskName(%v, %d) failed: %v", targetMap, lun, err))
		}

	} else if protocol == protocolFC {
		wwn, err := getFcWwnByTName(ctx, authClient, targetName)
		if err != nil {
			return "", status.Error(codes.InvalidArgument, fmt.Sprintf("getFcWwnByTName(%s) failed: %v", targetName, err))
		}
		klog.Infof("[getVolumeDiskName] targetName(%s) --> wwn(%s)", targetName, wwn)
		if wwn == "" {
			return "", status.Error(codes.Internal, fmt.Sprintf("Cant not get WWN of target(%s)", targetName))
		}

		if diskName, err = ns.getFcDiskName(wwn, lun, multipathEnabled); err != nil {
			klog.Errorf("[getVolumeDiskName] FC disk(%s) is not ready", diskName)
			return "", status.Error(codes.FailedPrecondition, fmt.Sprintf("getFcDiskName(%s, %d) failed: %v", wwn, lun, err))
		}
	}

	return diskName, nil
}

func (ns *NodeServer) getIscsiDiskName(targetMap map[string]string, lun uint64, chapUser, chapPasswd string, multipathEnabled bool) (string, error) {
	var tgts []*goiscsi.Target

	chap := &goiscsi.Chap{
		User:   chapUser,
		Passwd: chapPasswd,
	}
	for portal, target := range targetMap {
		target := &goiscsi.Target{
			Portal: formatIscsiPortal(portal),
			Name:   target,
			Lun:    lun,
			Chap:   chap,
		}
		tgts = append(tgts, target)
	}

	iscsi := &goiscsi.ISCSIUtil{Opts: goiscsi.ISCSIOptions{Timeout: DefaultIscsiTimeout, ForceMPIO: multipathEnabled}}
	err := iscsi.Login(tgts)
	if err != nil {
		return "", fmt.Errorf("failed to login iSCSI %+v: %v", tgts, err)
	}

	disk, err := iscsi.GetDisk(tgts)
	if err != nil {
		return "", fmt.Errorf("get iSCSI disk failed: %v", err)
	}
	klog.V(2).Infof("[getIscsiDiskName] Get disk: %+v", disk)
	for name, dev := range disk.Devices {
		klog.V(2).Infof("  %s: %+v\n", name, dev)
	}

	diskName := disk.Name
	diskValid := disk.Valid
	if disk.Status == "offline" {
		diskValid = false
	}

	if diskValid {
		return diskName, nil
	} else {
		return diskName, fmt.Errorf("iSCSI disk is invalid: %+v", disk)
	}
}

func (ns *NodeServer) getFcDiskName(wwn string, lun uint64, multipathEnabled bool) (string, error) {
	fc := &gofc.FCUtil{Opts: gofc.FCOptions{ForceMPIO: multipathEnabled}}
	disk, err := fc.GetDisk(wwn, lun)
	if err != nil {
		return "", fmt.Errorf("get FC disk(%s, %d) failed: %v", wwn, lun, err)
	}
	klog.V(2).Infof("[getFcDiskName] Get disk: %+v", disk)
	for name, dev := range disk.Devices {
		klog.V(4).Infof("  %s: %+v\n", name, dev)
	}

	diskName := disk.Name
	diskValid := disk.Valid
	if disk.Status == "offline" {
		diskValid = false
	}

	if diskValid {
		return diskName, nil
	} else {
		return diskName, fmt.Errorf("FC disk is invalid: %+v", disk)
	}
}

// NodeUnstageVolume unstage volume
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	stagingPath := req.GetStagingTargetPath()
	if len(stagingPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging path missing in request")
	}

	volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	if volData.protocol == protocolNFS {
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// default case: protocolISCSI and protocolFC
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	notMount, err := ns.mounter.IsLikelyNotMountPoint(stagingPath)
	if err != nil {
		klog.Warningf("[NodeUnstageVolume] Failed to get stagingPath(%s) mount point: %v\n", stagingPath, err)
		if os.IsNotExist(err) {
			klog.Infof("[NodeUnstageVolume] stagingPath(%s) not exists", stagingPath)
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		notMount = true
	}
	if !notMount {
		devName, refCnt, err := mount.GetDeviceNameFromMount(ns.mounter, stagingPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to GetDeviceNameFromMount(%s): %v", stagingPath, err)
		}
		klog.Infof("[NodeUnstageVolume] GetDeviceNameFromMount, devName(%s) refCnt(%d)", devName, refCnt)

		klog.Infof("[NodeUnstageVolume] unmount volume %s from %s", volumeID, stagingPath)
		if err = ns.mounter.Unmount(stagingPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", stagingPath, err)
		}

		if volData.protocol == protocolISCSI {
			iscsi := &goiscsi.ISCSIUtil{Opts: goiscsi.ISCSIOptions{Timeout: DefaultIscsiTimeout}}
			if err := iscsi.RemoveDisk(devName); err != nil {
				klog.Errorf("[NodeUnstageVolume] Remove device %s failed. err: %v", devName, err)
			}
		} else if volData.protocol == protocolFC {
			fc := &gofc.FCUtil{}
			if err := fc.RemoveDisk(devName); err != nil {
				klog.Errorf("[NodeUnstageVolume] Remove device %s failed. err: %v", devName, err)
			}
		}

	} else {
		klog.Warningf("[NodeUnstageVolume] stagingPath(%s) not unmounted", stagingPath)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume mount the volume
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	accessMode := volCap.GetAccessMode()
	if accessMode == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume access mode missing in request")
	}
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	stagingPath := req.GetStagingTargetPath()
	if len(stagingPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging path not provided")
	}
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	if volCap.GetBlock() != nil {
		return ns.publishBlockVolume(ctx, req, volCap, volumeID, stagingPath, targetPath)
	} else if volCap.GetMount() != nil {
		return ns.publishFilesystemVolume(ctx, req, volCap, volumeID, stagingPath, targetPath)
	}

	return nil, status.Error(codes.InvalidArgument, "Invalid volume capability")
}

func (ns *NodeServer) publishBlockVolume(ctx context.Context, req *csi.NodePublishVolumeRequest, volCap *csi.VolumeCapability, volumeID, stagingPath, targetPath string) (*csi.NodePublishVolumeResponse, error) {
	// if req.GetReadonly() {
	// 	return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Not support readonly for Block Volume ID %s", volumeID))
	// }

	volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[publishBlockVolume] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	authClient, err := ns.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	diskName, err := ns.getVolumeDiskName(ctx, authClient, req.GetPublishContext(), req.GetVolumeContext(), req.GetSecrets())
	if err != nil {
		return nil, err
	}

	// ns.mutex.Lock()
	// defer ns.mutex.Unlock()

	if volCap.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
		klog.Infof("[publishBlockVolume] Check diskName(%s) bind mount\n", diskName)
		if hasBindMnt(diskName) {
			return nil, status.Error(codes.FailedPrecondition, "ReadWriteOncePod conflict! The block volume was used by another pod.")
		}
	}

	// createTargetFile
	_, err = os.Lstat(targetPath)
	if os.IsNotExist(err) {
		klog.Infof("[publishBlockVolume] makeFile targetPath(%s)", targetPath)
		if err = makeFile(targetPath); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to makeFile target path: %v", err))
		}
	} else if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to check if the target block file exists: %v", err))
	}

	// checkAndMountBlockVolume
	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, status.Error(codes.Internal, fmt.Sprintf("error checking path %s for mount: %v", targetPath, err))
		}
		notMnt = true
	}
	if !notMnt {
		// It's already mounted.
		klog.Infof("Skipping bind-mounting subpath %s: already mounted", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	devPath := "/dev/" + diskName
	options := []string{"bind"}
	klog.Infof("[publishBlockVolume] volumeID(%v) devPath(%s) targetPath(%s) mountflags(%v)", volumeID, devPath, targetPath, options)
	if err := ns.mounter.Mount(devPath, targetPath, "", options); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount devPath: %s to %s: %v", devPath, targetPath, err))
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) publishFilesystemVolume(ctx context.Context, req *csi.NodePublishVolumeRequest, volCap *csi.VolumeCapability, volumeID, stagingPath, targetPath string) (*csi.NodePublishVolumeResponse, error) {
	if volCap.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
		mps, err := ns.mounter.GetMountRefs(stagingPath)
		if err != nil {
			fmt.Printf("failed to ListProcMounts for path %s, error: %v", stagingPath, err)
		}
		klog.Infof("[publishFilesystemVolume] stagingPath MountPoints: %+v \n", mps)
		// Default: /var/lib/kubelet/plugins/kubernetes.io/csi/xevo.csi.qsan.com/xxxx/globalmount is mount to
		// /host/var/lib/kubelet/plugins/kubernetes.io/csi/xevo.csi.qsan.com/xxxx/globalmount
		if len(mps) > 1 {
			return nil, status.Error(codes.FailedPrecondition, "volume uses the ReadWriteOncePod access mode and is already in use by another pod")
		}
	}

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("[publishFilesystemVolume] MkdirAll targetPath(%s)\n", targetPath)
			// Use MkdirAll instead of Mkdir for sanity test
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("create target path(%s) failed: %w", targetPath, err))
			}

			_, statErr := os.Stat(targetPath)
			if statErr != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("target path(%s) should exist but not found: %v", targetPath, statErr))
			}

			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, fmt.Sprintf("check target path: %v", err))
		}
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if req.VolumeContext["protocol"] == protocolNFS {
		options := req.GetVolumeCapability().GetMount().GetMountFlags()
		if req.GetReadonly() ||
			volCap.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
			options = append(options, "ro")
		}

		preferSec := ""
		sharePath := "/" + req.GetPublishContext()["shareName"]
		filteredOptions := make([]string, 0, len(options))
		for _, opt := range options {
			if strings.HasPrefix(opt, "nfsvers=") {
				ver := strings.TrimPrefix(opt, "nfsvers=")
				if contains(supportNfsVers, ver) {
					filteredOptions = append(filteredOptions, opt)
					if ver == "3" {
						sharePath = "/share/" + req.GetPublishContext()["shareName"]
					}
				} else {
					klog.Warningf("[publishFilesystemVolume] nfsvers(%s) Not supported. Use default nfs4.", ver)
				}
			} else if strings.HasPrefix(opt, "sec=") {
				sec := strings.TrimPrefix(opt, "sec=")
				if sec == "sys" {
					// Bypass "sys" security option
				} else if contains(supportKrb5Sec, sec) {
					preferSec = sec
				} else {
					klog.Warningf("[publishFilesystemVolume] sec(%s) Not supported.", sec)
				}
			} else {
				filteredOptions = append(filteredOptions, opt)
			}
		}
		options = filteredOptions

		volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
		}
		nfsPath := fmt.Sprintf("%s:%s", volData.server, sharePath)

		_, statErr := os.Stat(targetPath)
		if statErr != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("target path(%s) should exist but not found2: %v", targetPath, statErr))
		}

		// Resolve NFS security options
		options, err = ns.resolveNfsSecurity(ctx, volData, options, preferSec, req.GetVolumeContext()[paramSecurity])
		if err != nil {
			klog.Warningf("[publishFilesystemVolume] Failed to resolve NFS security: %v", err)
		}

		// Try RDMA NFS mount first
		if ns.tryMountWithRDMA(ctx, volData, volumeID, nfsPath, targetPath, options) {
			klog.Infof("[publishFilesystemVolume] RDMA NFS mount succeeded for volumeID(%v).", volumeID)
			return &csi.NodePublishVolumeResponse{}, nil
		}

		// Fallback to normal NFS mount
		klog.Infof("[publishFilesystemVolume] NFS volumeID(%v) nfsPath(%s) targetPath(%s) mountflags(%v)", volumeID, nfsPath, targetPath, options)
		if err := ns.mounter.Mount(nfsPath, targetPath, "nfs", options); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount device: %s at %s: %s", stagingPath, targetPath, err.Error()))
		}

	} else {
		options := []string{"bind"}
		if req.GetReadonly() {
			options = append(options, "ro")
		}

		klog.Infof("[publishFilesystemVolume] volumeID(%v) stagingPath(%s) targetPath(%s) mountflags(%v)", volumeID, stagingPath, targetPath, options)
		if err := ns.mounter.Mount(stagingPath, targetPath, "", options); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mount device: %s at %s: %s", stagingPath, targetPath, err.Error()))
		}
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmount the volume
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	// Unmount only if the target path is really a mount point.
	// if notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath); err != nil {
	if mnt, err := ns.mounter.IsMountPoint(targetPath); err != nil {
		klog.Warningf("[NodeUnpublishVolume] targetPath(%s) is not a mountpoint", targetPath)
		if !os.IsNotExist(err) {
			return nil, status.Errorf(codes.Internal, "Check targetPath(%s) mount point failed: %v", targetPath, err)
		}
	} else if mnt {
		fi, _ := os.Lstat(targetPath)
		klog.Infof("[NodeUnpublishVolume] targetPath(%s) Mode(%v)", targetPath, fi.Mode())
		if fi.Mode()&fs.ModeDevice != 0 {
			devName, err := GetDeviceNameFromTargetPath(targetPath)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			klog.Infof("[NodeUnpublishVolume] GetDeviceNameFromTargetPath, devName(%s)", devName)

			klog.Infof("[NodeUnpublishVolume] unmount block volume %s from %s", volumeID, targetPath)
			// Unmounting the image or filesystem.
			err = ns.mounter.Unmount(targetPath)
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to umount targetPath(%s), err: %v", targetPath, err))
			}

			needRemoveDisk := false
			mountPoints, err := getBindMounts(filepath.Base(devName))
			if err != nil {
				klog.Errorf("[NodeUnpublishVolume] getBindMounts(%s) failed. err: %v", filepath.Base(devName), err)
				needRemoveDisk = true
			} else {
				klog.Infof("[NodeUnpublishVolume] devBaseName(%s) mountCnt(%d)", filepath.Base(devName), len(mountPoints))
				if len(mountPoints) == 0 {
					needRemoveDisk = true
				}
			}

			if needRemoveDisk {
				volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
				if err != nil {
					return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
				}

				if volData.protocol == protocolISCSI {
					iscsi := &goiscsi.ISCSIUtil{Opts: goiscsi.ISCSIOptions{Timeout: DefaultIscsiTimeout}}
					if err := iscsi.RemoveDisk(devName); err != nil {
						klog.Errorf("[NodeUnpublishVolume] Remove device %s failed. err: %v", devName, err)
					}
				} else if volData.protocol == protocolFC {
					fc := &gofc.FCUtil{}
					if err := fc.RemoveDisk(devName); err != nil {
						klog.Errorf("[NodeUnpublishVolume] Remove device %s failed. err: %v", devName, err)
					}
				}
			}
		} else {

			klog.Infof("[NodeUnpublishVolume] unmount file volume %s from %s", volumeID, targetPath)
			// Unmounting the image or filesystem.
			err := ns.mounter.Unmount(targetPath)
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to umount targetPath(%s), err: %v", targetPath, err))
			}
		}
	} else {
		klog.Infof("[NodeUnpublishVolume] WARNING!! targetPath(%s) mnt(%v) err: %v", targetPath, mnt, err)
	}

	// Delete the mount point.
	// Does not return error for non-existent path, repeated calls OK for idempotency.
	if err := os.RemoveAll(targetPath); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to remove targetPath(%s), err: %v", targetPath, err))
	}
	klog.Infof("[NodeUnpublishVolume] Volume ID %s has been unpublished.", volumeID)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetInfo return info of the node on which this plugin is running
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	topology := map[string]string{}
	nameArr := GetFcTargetNodeNames()
	if len(nameArr) > 0 {
		topology["topology.qsan.com/fc"] = strings.Join(nameArr, "_")
	} else {
		topology["topology.qsan.com/fc"] = "none"
	}

	// For e2e topology test
	topology["topology.qsan.com/model"] = ns.Driver.nodeID

	return &csi.NodeGetInfoResponse{
		NodeId: ns.Driver.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: topology,
		},
		MaxVolumesPerNode: ns.Driver.maxVolumesPerNode,
	}, nil
}

// NodeGetCapabilities return the capabilities of the Node plugin
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.Driver.nscap,
	}, nil
}

// NodeGetVolumeStats get volume stats
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}

	klog.Infof("[NodeGetVolumeStats] volumeID(%s) volumePath(%s)", volumeID, volumePath)
	if mnt, err := ns.mounter.IsMountPoint(volumePath); err != nil {
		return nil, status.Errorf(codes.NotFound, "Could not get mount information from %s: %+v", volumePath, err)
	} else if !mnt {
		return nil, status.Errorf(codes.NotFound, "volumePath(%s) not mount", volumePath)
	}

	volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
	// klog.Infof("[NodeGetVolumeStats] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	authClient, err := ns.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var gvol *genericVolumeData
	if volData.protocol == protocolNFS {
		volumeAPI := goqsan.NewFileVolume(authClient)
		vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
		if err != nil {
			resterr, ok := err.(*goqsan.RestError)
			if ok && resterr.StatusCode == http.StatusNotFound {
				klog.Warningf("[NodeGetVolumeStats] Volume ID %s not found at the server.", volumeID)
				return &csi.NodeGetVolumeStatsResponse{
					VolumeCondition: &csi.VolumeCondition{
						Abnormal: true,
						Message:  "Volume is not found",
					},
				}, nil
			}

			klog.Warningf("[NodeGetVolumeStats] GetVolume(%s) failed with StatusCode(%d). err: %v", volumeID, resterr.StatusCode, err)
			return &csi.NodeGetVolumeStatsResponse{
				VolumeCondition: &csi.VolumeCondition{
					Abnormal: true,
					Message:  err.Error(),
				},
			}, nil
		}

		gvol = convertToGenericVolumeData(vol)

	} else {
		volumeAPI := goqsan.NewVolume(authClient)
		vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
		if err != nil {
			resterr, ok := err.(*goqsan.RestError)
			if ok && resterr.StatusCode == http.StatusNotFound {
				klog.Warningf("[NodeGetVolumeStats] Volume ID %s not found at the server.", volumeID)
				return &csi.NodeGetVolumeStatsResponse{
					VolumeCondition: &csi.VolumeCondition{
						Abnormal: true,
						Message:  "Volume is not found",
					},
				}, nil
			}

			klog.Warningf("[NodeGetVolumeStats] GetVolume(%s) failed with StatusCode(%d). err: %v", volumeID, resterr.StatusCode, err)
			return &csi.NodeGetVolumeStatsResponse{
				VolumeCondition: &csi.VolumeCondition{
					Abnormal: true,
					Message:  err.Error(),
				},
			}, nil
		} else {
			if vol.TargetID == "" {
				klog.Warningf("[NodeGetVolumeStats] Volume ID %s not attach at the server.", volumeID)
				return &csi.NodeGetVolumeStatsResponse{
					VolumeCondition: &csi.VolumeCondition{
						Abnormal: true,
						Message:  "Volume is not attached",
					},
				}, nil
			}
		}

		gvol = convertToGenericVolumeData(vol)
	}

	abnormal, msg := getVolumeCondition(gvol)
	availSize := int64(gvol.TotalSize - gvol.UsedSize)
	if availSize < 0 {
		availSize = 0
	}

	klog.Infof("[NodeGetVolumeStats] Name(%s) Abnormal(%v) Online(%v) Health(%s) TotalSize(%d) UsedSize(%d)", gvol.Name, abnormal, gvol.Online, gvol.Health, gvol.TotalSize, gvol.UsedSize)
	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			&csi.VolumeUsage{
				Available: availSize,
				Total:     int64(gvol.TotalSize),
				Used:      int64(gvol.UsedSize),
				Unit:      csi.VolumeUsage_BYTES,
			},
		},
		VolumeCondition: &csi.VolumeCondition{
			Abnormal: abnormal,
			Message:  msg,
		},
	}, nil
}

// NodeExpandVolume node expand volume
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}

	volData, err := ns.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[NodeExpandVolume] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	if volData.protocol == protocolNFS {
		return &csi.NodeExpandVolumeResponse{CapacityBytes: req.GetCapacityRange().GetRequiredBytes()}, nil
	}

	// default case: protocolISCSI and protocolFC
	authClient, err := ns.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	volumeAPI := goqsan.NewVolume(authClient)
	vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume ID %s not found.", volData.volId))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("[NodeExpandVolume] vol: %+v", vol)

	newSize := req.GetCapacityRange().GetRequiredBytes()
	if uint64(newSize>>20) != vol.TotalSize {
		klog.Warningf("[NodeExpandVolume] Expand size mismatch ! (%u vs %u)", (newSize >> 20), vol.TotalSize)
	}

	lunData, err := getLunDataByVolumeId(ctx, authClient, volData.volId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("[NodeExpandVolume] lunData %+v", lunData)

	iscsi := &goiscsi.ISCSIUtil{Opts: goiscsi.ISCSIOptions{Timeout: DefaultIscsiTimeout}}
	fc := &gofc.FCUtil{}
	if req.GetVolumeCapability().GetBlock() != nil {
		// If a device-mapper device exists for VD WWN, resize the multipath device.
		if matches, err := filepath.Glob("/dev/mapper/?" + vol.Tags.Wwn); err == nil {
			if len(matches) > 1 {
				klog.Warningf("[NodeExpandVolume] WWN(%s) matches more than one dm device: %v", vol.Tags.Wwn, matches)
			}
			for _, devName := range matches {
				klog.Infof("[NodeExpandVolume] Expand block device %s", devName)
				if volData.protocol == protocolISCSI {
					iscsi.ExpandDisk(devName, lunData.Name)
				} else if volData.protocol == protocolFC {
					fc.ExpandDisk(devName, lunData.Name)
				}
			}
		} else {
			klog.Infof("[NodeExpandVolume] Rescan All sessions for block device")
			if volData.protocol == protocolISCSI {
				iscsi.RescanAllSessions()
			} else if volData.protocol == protocolFC {
				fc.RescanHost()
			}
		}

	} else if req.GetVolumeCapability().GetMount() != nil {
		devName, refCnt, err := mount.GetDeviceNameFromMount(ns.mounter, volumePath)
		if err != nil {
			klog.Warningf("[NodeExpandVolume] GetDeviceNameFromMount failed: %v", err)
		}
		klog.Infof("[NodeExpandVolume] Expand mounted device %s, refCnt(%d)", devName, refCnt)

		if volData.protocol == protocolISCSI {
			iscsi.ExpandDisk(devName, lunData.Name)
		} else if volData.protocol == protocolFC {
			fc.ExpandDisk(devName, lunData.Name)
		}

		if err := ns.ResizeDeviceFilesystem(devName, volumePath); err != nil {
			return nil, status.Errorf(codes.Internal, "[NodeExpandVolume] ResizeDeviceFilesystem(%s, %s) failed: %v", devName, volumePath, err)
		}

	}

	return &csi.NodeExpandVolumeResponse{CapacityBytes: newSize}, nil
}

func (ns *NodeServer) ResizeDeviceFilesystem(devName, devMountPath string) error {
	klog.Infof("[ResizeDeviceFilesystem] Sync and ResizeFs filesystem(%s) of %s", devMountPath, devName)

	if err := syncfs(devMountPath); err != nil {
		klog.Warningf("[ResizeDeviceFilesystem] Error syncing devName(%s) filesystem: %v", devName, err)
	}

	showMountFsSize(devMountPath)
	fs := mount.NewResizeFs(ns.mounter.Exec)
	if _, err := fs.Resize(devName, devMountPath); err != nil {
		return fmt.Errorf("Failed to resize %s file system: %v", devName, err)
	}

	// Check if devMountPath file system needs to be resized again.
	// If needed, resize the file system using devName device.
	needResize, err := fs.NeedResize(devName, devMountPath)
	if err != nil {
		klog.Warningf("[ResizeDeviceFilesystem] failed to check if %s file system needs resize: %v", devName, err)
	}
	if needResize {
		klog.Infof("[ResizeDeviceFilesystem] ResizeFs %s with %s again", devName, devMountPath)
		_, err = fs.Resize(devName, devMountPath)
		if err != nil {
			return fmt.Errorf("Failed to resize %s file system again: %v", devName, err)
		}
	}
	showMountFsSize(devMountPath)

	return nil
}

// resolveNfsSecurity analyzes the security list and checks if krb5 is enabled on the storage.
// It returns the updated NFS mount options.
func (ns *NodeServer) resolveNfsSecurity(
	ctx context.Context,
	volData *volumeContextData,
	options []string,
	preferSec string,
	securityStr string,
) ([]string, error) {

	if securityStr == "" {
		klog.Infof("[resolveNfsSecurity] No security option provided, default to sys")
		return options, nil
	}
	
	securityList := strings.Split(securityStr, ":")
	sec := "sys"
	if contains(securityList, "krb5p") {
		sec = "krb5p"
	} else if contains(securityList, "krb5i") {
		sec = "krb5i"
	} else if contains(securityList, "krb5") {
		sec = "krb5"
	}

	// If preferSec is specified and exists in the securityList, use it.
	if preferSec != "" && contains(securityList, preferSec) {
		sec = preferSec
	}

	if strings.HasPrefix(sec, "krb5") {
		authClient, err := ns.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
		if err != nil {
			return options, status.Error(codes.Internal, err.Error())
		}
		sharehostAPI := goqsan.NewSharehost(authClient)
		keytab, err := sharehostAPI.GetKeytab(ctx)
		if err != nil {
			return options, fmt.Errorf("[resolveNfsSecurity] Failed to GetKeytab: %v", err)
		}
		if keytab.Enable {
			klog.Infof("[resolveNfsSecurity] Enable Kerberos with %s (prefer[%s]) from securityList %v", sec, preferSec, securityList)
			options = append(options, "sec="+sec)
		} else {
			klog.Warningf("[resolveNfsSecurity] Kerberos is not enabled on the storage, but securityList %v contains krb5", securityList)
		}
	}

	return options, nil
}

// tryMountWithRDMA tries to mount the NFS volume using RDMA protocol first.
// If RDMA is not available or fails, it returns false and does not modify the mount state.
func (ns *NodeServer) tryMountWithRDMA(
	ctx context.Context,
	volData *volumeContextData,
	volumeID, nfsPath, targetPath string,
	options []string,
) bool {

	if !hasRDMADevice() {
		return false
	}

	authClient, err := ns.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return false
	}

	if !IsServerSupportRDMA(ctx, authClient, volData.server) {
		return false
	}

	rdmaOptions := append([]string{}, options...)
	rdmaOptions = append(rdmaOptions, "proto=rdma")

	klog.Infof("[tryMountWithRDMA] Trying RDMA mount for volumeID(%v) nfsPath(%s) targetPath(%s) options(%v)",
		volumeID, nfsPath, targetPath, rdmaOptions)

	if err := ns.mounter.Mount(nfsPath, targetPath, "nfs", rdmaOptions); err != nil {
		klog.Warningf("[tryMountWithRDMA] RDMA mount failed for volumeID(%v). Falling back to normal NFS. err: %v",
			volumeID, err)
		return false
	}

	// klog.Infof("[tryMountWithRDMA] RDMA mount succeeded for volumeID(%v).", volumeID)
	return true
}

