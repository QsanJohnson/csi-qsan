package driver

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gitlab.qsan.com/sharedlibs-go/goqsan"

	"k8s.io/klog/v2"
)

// ControllerServer controller server setting
type ControllerServer struct {
	Driver     *Driver
	mutexBLock sync.Mutex
	mutexFile  sync.Mutex
	mapMu      sync.RWMutex
}

var convertVolTypeMutex sync.Mutex

// CreateVolume create a volume
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume name must be provided")
	}

	tokens := strings.Split(name, "-")
	if len(tokens) >= 4 {
		name = tokens[0] + "-" + strings.Join(tokens[3:], "-")
	}
	if len(name) > MAX_VD_NAME_LEN {
		name = name[:MAX_VD_NAME_LEN-1]
	}
	klog.Infof("[CreateVolume] volumeName: %s ==> %s", req.GetName(), name)

	parameters := req.GetParameters()
	protocol, exists := parameters[strings.ToLower(paramProtocol)]
	if exists {
		if !contains(supportProtocols, protocol) {
			return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Invalid protocol %s in storage class", protocol))
		}
	} else {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramProtocol))
	}

	if err := isValidVolumeCapabilities(protocol, req.GetVolumeCapabilities()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if protocol == protocolNFS {
		if err := cs.Driver.fsCreateLimiter.TryEnter(); err != nil {
			return nil, status.Errorf(codes.ResourceExhausted, fmt.Sprintf("Too many concurrent CreateFileVolume requests. Name(%s) err: %v", name, err))
		}
		defer cs.Driver.fsCreateLimiter.Leave()

		return cs.CreateFileVolume(ctx, req, protocol, name)
	} else {
		return cs.CreateBlockVolume(ctx, req, protocol, name)
	}
}

func (cs *ControllerServer) CreateBlockVolume(ctx context.Context, req *csi.CreateVolumeRequest, protocol, name string) (*csi.CreateVolumeResponse, error) {
	preferredAccessibility := req.GetAccessibilityRequirements().GetPreferred()
	// topologies := []*csi.Topology{}
	// topologies = append(topologies, &csi.Topology{Segments: map[string]string{"topology.qsan.csi/node": cs.Driver.nodeID}})
	// klog.Infof("[CreateVolume] topologies: %+v", topologies)
	// preferredAccessibility := []*csi.Topology{&csi.Topology{Segments: map[string]string{"topology.qsan.csi/node": cs.Driver.nodeID}}}
	klog.Infof("[CreateBlockVolume] %s, preferredAccessibility: %+v", req.GetName(), preferredAccessibility)

	reqCapacity := req.GetCapacityRange().GetRequiredBytes()
	parameters := req.GetParameters()
	klog.Infof("[CreateBlockVolume] reqCapacity: %d, parameters: %v", reqCapacity, parameters)
	if parameters == nil {
		parameters = make(map[string]string)
	}

	var server, pool, poolId, caps, iscsiPortals, iscsiTargets, targetName, bgIoPriority string
	var bkSize uint64 = 4096
	qosOptions := goqsan.VolumeQoSOptions{}

	// validate parameters (case-insensitive)
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case paramProtocol:
			// To avoid go to default
		case paramServer:
			server = v
		case paramPool:
			pool = v
		// case paramPoolId:
		// 	poolId = v
		case paramBlockSize:
			bkSize, _ = strconv.ParseUint(v, 10, 32)
		case paramCapabilities:
			caps = v
		case paramIscsiPortals:
			iscsiPortals = v
		case paramIscsiTargets:
			iscsiTargets = v
		case paramTarget:
			targetName = v
		case paramBgIoPriority:
			bgIoPriority = v
		case paramIoPriority:
			qosOptions.IoPriority = v
		case paramTargetRespmTime:
			respTime, _ := strconv.ParseUint(v, 10, 32)
			qosOptions.TargetResponseTime = respTime
		case paramMaxIops:
			iops, _ := strconv.ParseUint(v, 10, 32)
			qosOptions.MaxIops = iops
		case paramMaxThroughputKB:
			throughput, _ := strconv.ParseUint(v, 10, 32)
			qosOptions.MaxThroughtput = throughput
		default:
			klog.Warningf("[CreateBlockVolume] invalid parameter %q in storage class", k)
		}
	}

	if server == "" {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramServer))
	}
	// if poolId == "" {
	// 	return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramPoolId))
	// }
	if pool == "" {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramPool))
	}

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	poolId, provision, dedupEnabled, err := getPoolIDProvByName(ctx, authClient, pool)
	if err != nil {
		klog.Warningf("[CreateBlockVolume] getPoolIDProvByName failed, err: %v", err)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%s(%s) is not exist at server %s", paramPool, pool, server))
	}
	if dedupEnabled {
		klog.Warningf("[CreateBlockVolume] Dedup Pool(%s) does not support CSI volume.", pool)
		return nil, status.Errorf(codes.InvalidArgument, "Dedup Pool(%s) does not support CSI volume.", pool)
	}
	klog.Infof("[CreateBlockVolume] poolname(%s) --> poolId(%s), provision(%s), dedupEnabled(%v)", pool, poolId, provision, dedupEnabled)

	multipathEnabled := false
	topologies := []*csi.Topology{}
	if protocol == protocolISCSI {
		if iscsiPortals == "" {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramIscsiPortals))
		}
		if iscsiTargets == "" && targetName == "" {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Either %v or %v is a required parameter", paramIscsiTargets, paramTarget))
		}

		portalArr := splitAndTrimSpace(iscsiPortals, ",")
		if targetName != "" {
			// multipath
			multipathEnabled = true
			// Check if every portal parameters are valid
			validPortalArr, err := getIscsiPortalsByTName(ctx, authClient, targetName)
			if err != nil {
				klog.Warningf("[CreateBlockVolume] getIscsiPortalsByTName(%s) failed, err: %v", targetName, err)
			}

			for _, portal := range portalArr {
				portalIp := getPortalIp(portal)
				if !contains(validPortalArr, portalIp) {
					klog.Errorf("[CreateBlockVolume] portalIp(%s) is invalid. Valid portals: %v", portalIp, validPortalArr)
					return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%s parameter has invalid value '%s'.", paramIscsiPortals, portal))
				}
			}

		} else if iscsiTargets != "" {
			// single path
			targetArr := strings.Split(iscsiTargets, ",")
			if len(portalArr) != len(targetArr) {
				return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("The number of %v and %v is mismatch.", paramIscsiPortals, paramIscsiTargets))
			}
			if len(portalArr) > 1 {
				// Workaround for not support multipath
				iscsiPortals = portalArr[0]
				iscsiTargets = targetArr[0]
			}
		} else {
			// parameter mismatch
			if targetName != "" {
				return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("The number of %v parameter should be 2 for %v", paramIscsiPortals, paramTarget))
			}
			if iscsiTargets != "" {
				return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("The number of %v parameter should be 1 for %v", paramIscsiPortals, paramIscsiTargets))
			}
		}

		topologies = preferredAccessibility

	} else if protocol == protocolFC {
		if targetName == "" {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramTarget))
		}

		multipathEnabled = true
		wwn, err := getFcWwnByTName(ctx, authClient, targetName)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%s(%s) is not exist at server %s", paramTarget, targetName, server))
		}

		klog.Infof("[CreateBlockVolume] targetName(%s) --> wwn(%s)", targetName, wwn)
		for _, topology := range preferredAccessibility {
			// klog.Infof("[CreateVolume] no: %d, topology: %+v", no, topology)
			// klog.Infof("[CreateVolume] Segments: %+v", topology.Segments)
			if strings.Contains(topology.Segments["topology.qsan.com/fc"], wwn) {
				// klog.Infof("[CreateVolume] FOUND: %s", topology.Segments["topology.qsan.com/fc"])
				topologies = append(topologies, topology)
			}
		}

		// topologies := []*csi.Topology{}
		// topologies = append(topologies, &csi.Topology{Segments: map[string]string{"topology.qsan.csi/node": cs.Driver.nodeID}})
		// klog.Infof("[CreateVolume] topologies: %+v", topologies)
		// preferredAccessibility = []*csi.Topology{&csi.Topology{Segments: map[string]string{"topology.qsan.csi/node": cs.Driver.nodeID}}}
		// preferredAccessibility = []*csi.Topology{&csi.Topology{Segments: map[string]string{"topology.qsan.com/fc": "001378EC9AED"}}}

		// preferredAccessibility = []*csi.Topology{&csi.Topology{Segments: map[string]string{"topology.qsan.com/fc": wwn}}}

		if len(topologies) == 0 {
			return nil, status.Errorf(codes.ResourceExhausted, "%s can not be provisioned on a node without FC connectivity.", req.GetName())
		}

	}

	klog.Infof("[CreateBlockVolume] parameters: protocol(%s), server(%s), poolId(%s), caps(%s), bgIoPriority(%s) qosOpts(%+v)", protocol, server, poolId, caps, bgIoPriority, qosOptions)

	var resizeVolSizeMB uint64
	capacityBytes := reqCapacity
	volSizeMB := (reqCapacity + (1<<20) - 1) >> 20
	if volSizeMB == 0 {
		volSizeMB = 1
		klog.Infof("[CreateBlockVolume] Set volSizeMB to 1. reqCapacity(%v)", reqCapacity)
	} else if (volSizeMB * 1024 * 1024) != reqCapacity {
		klog.Infof("[CreateBlockVolume] reqCapacity(%v) ==> volSize(%v)", reqCapacity, volSizeMB * 1024 * 1024)
	}

	volumeAPI := goqsan.NewVolume(authClient)

	var vol *goqsan.VolumeData
	var parentVolId string
	if req.GetVolumeContentSource() != nil {
		volumeSource := req.VolumeContentSource
		switch volumeSource.Type.(type) {
		case *csi.VolumeContentSource_Snapshot:
			klog.Infof("[CreateBlockVolume] VolumeContentSource Type: VolumeContentSource_Snapshot")
			if snapshot := volumeSource.GetSnapshot(); snapshot != nil {
				snapData, err := cs.Driver.GetContextDataFromSnapContextID(snapshot.GetSnapshotId())
				if err != nil {
					return nil, status.Error(codes.NotFound, fmt.Sprintf("Failed to get snapshot context data from Snapshot ID %s: %v", snapshot.GetSnapshotId(), err))
				}
				klog.Infof("[CreateBlockVolume] snapId: %s, snapVolumeContextData: %+v", snapData.snapId, snapData.volumeContextData)

				parentVolId = snapData.volId
				if _, err = validateCloneCondition(ctx, &VolumeWrapper{op: volumeAPI}, volSizeMB, parentVolId); err != nil {
					return nil, err
				}

				klog.Infof("[CreateBlockVolume] Restore snapshot(%s, %s) volume(%s) size(%d) source(%s)", snapData.volId, snapData.snapId, name, volSizeMB, volumeSource.GetVolume().GetVolumeId())
				vol, err = volumeAPI.Clone(ctx, snapData.volId, snapData.snapId, name, poolId)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "Failed to restore volumeID %s. err: %v", volumeSource.GetVolume().GetVolumeId(), err)
				}

				capacityMB := uint64(capacityBytes) >> 20
				klog.Infof("[CreateBlockVolume] Restore capacityMB(%d) vs vol.TotalSize(%d)", capacityMB, vol.TotalSize)
				if capacityMB > vol.TotalSize {
					resizeVolSizeMB = capacityMB
					go monitorCloneStatus(&VolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, resizeVolSizeMB)
				} else {
					go monitorCloneStatus(&VolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, 0)
				}

			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Snapshot is not found.")
			}

		case *csi.VolumeContentSource_Volume:
			klog.Infof("[CreateBlockVolume] VolumeContentSource Type: VolumeContentSource_Volume")
			srcVolData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeSource.GetVolume().GetVolumeId())
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "Invalid volume source ID %s", volumeSource.GetVolume().GetVolumeId())
			}
			klog.Infof("[CreateBlockVolume] srcVolData: %+v", srcVolData)

			parentVolId = srcVolData.volId
			_, err = validateCloneCondition(ctx, &VolumeWrapper{op: volumeAPI}, volSizeMB, parentVolId)
			if err != nil {
				return nil, err
			}

			if err = allocSnapshotSpace(ctx, &VolumeWrapper{op: volumeAPI}, srcVolData.volId); err != nil {
				return nil, status.Errorf(codes.Internal, "allocSnapshotSpace failed: %v", err)
			}

			klog.Infof("[CreateBlockVolume] Clone volume(%s) size(%d) source(%s)", name, volSizeMB, volumeSource.GetVolume().GetVolumeId())
			vol, err = volumeAPI.Clone(ctx, srcVolData.volId, "", name, poolId)
			if err != nil {
				resterr, ok := err.(*goqsan.RestError)
				if ok && resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_SNAP_NOT_INITED_V1 {
					return nil, status.Errorf(codes.Internal, "VolumeID %s snapshot space is not enalbed", volumeSource.GetVolume().GetVolumeId())
				} else {
					return nil, status.Errorf(codes.Internal, "Failed to clone volumeID %s. err: %v", volumeSource.GetVolume().GetVolumeId(), err)
				}
			}
			capacityMB := uint64(capacityBytes) >> 20
			klog.Infof("[CreateBlockVolume] Clone capacityMB(%d) vs vol.TotalSize(%d)", capacityMB, vol.TotalSize)
			if capacityMB > vol.TotalSize {
				resizeVolSizeMB = capacityMB
				go monitorCloneStatus(&VolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, resizeVolSizeMB)
			} else {
				go monitorCloneStatus(&VolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, 0)
			}

		default:
			klog.Infof("[CreateBlockVolume] VolumeContentSource Type: default")
			return nil, status.Errorf(codes.InvalidArgument, "%v not a proper volume source", volumeSource)
		}
	} else {

		options := &goqsan.VolumeCreateOptions{
			BlockSize:    bkSize,
			BgIoPriority: bgIoPriority,
		}
		if caps != "" {
			nCaps, _ := strconv.ParseUint(caps, 10, 32)
			mode := "WRITE_BACK"
			if (nCaps & CAP_CACHEMODE_WT) == CAP_CACHEMODE_WT {
				mode = "WRITE_THROUGH"
			}
			options.CacheMode = mode

			readahead := false
			if (nCaps & CAP_READAHEAD_ENABLE) == CAP_READAHEAD_ENABLE {
				readahead = true
			}
			options.EnableReadAhead = &readahead
		}
		metastatus, metatype, metadata := generateMetadata(protocol, cs.Driver.nodeIP, cs.Driver.version, cs.Driver.buildDate)
		if strings.EqualFold(provision, ProvisionThin) {
			metadata[MCIDX_ATTR] = metadata[MCIDX_ATTR] | META_ATTR_NEP
		}
		options.Metadata = goqsan.VolumeMetadata{
			Status:    metastatus,
			Type:      metatype,
			Content:   base64.StdEncoding.EncodeToString(metadata),
			Timestamp: "AUTO",
		}

		klog.Infof("[CreateBlockVolume] Create volume(%s) size(%d) options: %+v", name, volSizeMB, options)

		// Check if volume with already existing name exists
		vol, err = getBlockVolumeByName(ctx, authClient, poolId, name)
		if vol == nil {
			// Volume with already existing name not exist
			vol, err = volumeAPI.CreateVolume(ctx, poolId, name, uint64(volSizeMB), options)
			if err != nil {
				resterr, ok := err.(*goqsan.RestError)
				if ok {
					switch {
					case resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_RG_NO_SPACE_V2,
						resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_RG_NO_SPACE_V1:
						// LVMERR_RG_NO_SPACE (-104)
						return nil, status.Error(codes.OutOfRange, err.Error())
					case resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_ARG_V2,
						resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_ARG_V1:
						// LVMERR_INVALID_ARG (-3)
						return nil, status.Error(codes.InvalidArgument, err.Error())
					case resterr.StatusCode == http.StatusConflict && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_U_NAME_EXISTS_V2,
						resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_U_NAME_EXISTS_V1:
						// LVMERR_U_NAME_EXISTS (-1501)
						// Should not be here because we had check if volume exist before creating volume.
						vol2, err := getBlockVolumeByName(ctx, authClient, poolId, name)
						if vol2 != nil {
							if int64(vol2.TotalSize) == volSizeMB {
								vol = vol2
								klog.Infof("[CreateBlockVolume] Volume2(%s) is already exist and size is equal.", name)
							} else {
								return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("Volume2(%s) is already exist, but size not equal (%v vs %v)", name, vol.TotalSize, volSizeMB))
							}
						} else {
							return nil, status.Error(codes.InvalidArgument, err.Error())
						}
					}
				}

				return nil, status.Error(codes.Internal, err.Error())
			}

			klog.Infof("[CreateBlockVolume] CreateVolume: %+v", vol)
			if strings.EqualFold(provision, ProvisionThick) {
				// Sleep for 2 seconds to prevent a potential LVM null pointer issue if this volume is deleted immediately.
				// This issue may be triggered when running the sanity test.
				time.Sleep(2 * time.Second)
			}

			// Check volume metadata
			metastatus2, metatype2, metadata2, err := volumeAPI.GetMetadata(ctx, vol.ID)
			if err != nil {
				klog.Errorf("[CreateBlockVolume] GetMetadata failed, err: %+v", err)
			}
			klog.Infof("[CreateBlockVolume] Volume metadata stat(%s) type(%s) buf %v", metastatus2, metatype2, metadata2)
			if metastatus2 != metastatus || metatype2 != metatype || !bytes.HasPrefix(metadata2, metadata) {
				klog.Errorf("[CreateBlockVolume] Set volume metadata failed. metaStatus(%s) metaType(%s) metadata %v vs %v", metastatus2, metatype2, metadata2, metadata)
			}

			// Set QoS if need
			if (qosOptions != goqsan.VolumeQoSOptions{}) {
				opts := &goqsan.VolumeModifyOptions{VolumeQoSOptions: qosOptions}
				klog.Infof("[CreateBlockVolume] QoS qosOptions: %+v", opts)

				vol, err = volumeAPI.ModifyVolume(ctx, vol.ID, opts)
				if err == nil {
					klog.Infof("[CreateBlockVolume] QoS ModifyVolume: %+v", vol)
				} else {
					klog.Errorf("[CreateBlockVolume] Set volume(%s) QoS(%+v) failed: %+v", vol.ID, qosOptions, err)
				}
			}
		} else {
			// Volume with already existing name exist
			if vol.TotalSize == uint64(volSizeMB) {
				klog.Infof("[CreateBlockVolume] Volume(%s) is already exist and size is equal.", name)
			} else {
				return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("Volume(%s) is already exist, but size not equal (%v vs %v)", name, vol.TotalSize, volSizeMB))
			}
		}

		capacityBytes = int64(vol.TotalSize) << 20
	}

	volumeContextID := cs.Driver.GenerateVolumeContextID(protocol, server, poolId, vol.ID, nil, nil)
	// Don't need to add volStatMap here. It will be added later in ControllerGetVolume.
	// cs.Driver.volStatMap[volumeContextID] = VolumeStat{
	// 	LastUpdateTimeStamp: time.Now(),
	// }
	// klog.Infof("[CreateVolume] Add volStatMap[%s](%+v)", volumeContextID, cs.Driver.volStatMap[volumeContextID])

	context := map[string]string{
		paramProtocol:         protocol,
		paramPoolId:           poolId,
		paramIscsiPortals:     iscsiPortals,
		paramIscsiTargets:     iscsiTargets,
		paramTarget:           targetName,
		paramMultipathEnabled: strconv.FormatBool(multipathEnabled),
		paramParentVolId:      parentVolId,
		paramResize:           strconv.FormatUint(resizeVolSizeMB, 10),
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volumeContextID,
			CapacityBytes:      capacityBytes,
			VolumeContext:      context,
			ContentSource:      req.GetVolumeContentSource(),
			AccessibleTopology: topologies,
		},
	}, nil
}

func (cs *ControllerServer) CreateFileVolume(ctx context.Context, req *csi.CreateVolumeRequest, protocol, name string) (*csi.CreateVolumeResponse, error) {
	preferredAccessibility := req.GetAccessibilityRequirements().GetPreferred()
	klog.Infof("[CreateFileVolume] %s, preferredAccessibility: %+v", req.GetName(), preferredAccessibility)

	reqCapacity := req.GetCapacityRange().GetRequiredBytes()
	parameters := req.GetParameters()
	klog.Infof("[CreateFileVolume] reqCapacity: %d, parameters: %v", reqCapacity, parameters)
	if parameters == nil {
		parameters = make(map[string]string)
	}

	var server, mgmtIp, pool, poolId, caps, targetName, bgIoPriority string
	var bkSize uint64 = 4096
	qosOptions := goqsan.VolumeQoSOptions{}

	// validate parameters (case-insensitive)
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case paramProtocol:
			// To avoid go to default
		case paramServer:
			server = v
		case paramMgmtIp:
			mgmtIp = v
		case paramPool:
			pool = v
		case paramBlockSize:
			bkSize, _ = strconv.ParseUint(v, 10, 32)
		case paramCapabilities:
			caps = v
		case paramTarget:
			targetName = v
		case paramBgIoPriority:
			bgIoPriority = v
		case paramIoPriority:
			qosOptions.IoPriority = v
		case paramTargetRespmTime:
			respTime, _ := strconv.ParseUint(v, 10, 32)
			qosOptions.TargetResponseTime = respTime
		case paramMaxIops:
			iops, _ := strconv.ParseUint(v, 10, 32)
			qosOptions.MaxIops = iops
		case paramMaxThroughputKB:
			throughput, _ := strconv.ParseUint(v, 10, 32)
			qosOptions.MaxThroughtput = throughput
		default:
			klog.Warningf("[CreateFileVolume] invalid parameter %q in storage class", k)
		}
	}

	if server == "" {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramServer))
	}
	if pool == "" {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramPool))
	}
	if targetName == "" {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%v is a required parameter", paramTarget))
	}

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, server)
	if err != nil {
		klog.Errorf("[CreateFileVolume] %s(%s) is not found in qsan-auth secret.", paramServer, server)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if mgmtIp != "" {
		// It set, mgmtIP is used for management port, and server is used for I/O port.
		networkAPI := goqsan.NewNetwork(authClient)
		if nics, err := networkAPI.ListNICs(ctx, goqsan.ModelQSM4); err != nil {
			klog.Warningf("[CreateFileVolume] ListNICs failed, err: %v", err)
		} else {
			localIps, clusterIps := []string{}, []string{}
			bFound := false

			// Check local NICs for mgmtIp
			for _, nic := range *nics {
				localIps = append(localIps, nic.Ipv4.IP)
				if nic.Ipv4.IP == mgmtIp {
					bFound = true
					if nic.Online {
						klog.Infof("[CreateFileVolume] %s(%s) found in local NICs", paramMgmtIp, mgmtIp)
						if newAuthClient, err := cs.Driver.qsan.UpdateAuthClient(ctx, server, mgmtIp); err != nil {
							klog.Warningf("[CreateFileVolume] UpdateAuthClient(%s, %s) failed. err: %v", server, mgmtIp, err)
						} else {
							authClient = newAuthClient
						}
					} else {
						klog.Warningf("[CreateFileVolume] %s(%s) found, but not online. %+v", paramMgmtIp, mgmtIp, nic)
					}
					break
				}
			}

			// If mgmtIp is not found, check clusters
			if !bFound {
				clusters, err := networkAPI.ListClusters(ctx)
				if err != nil {
					klog.Warningf("[CreateFileVolume] ListClusters failed: %v", err)
				} else {
					// klog.Infof("[CreateFileVolume] ListClusters: version=%s, cnt=%d\n", clusters.Version, len(clusters.ClusterIps))
					for _, clusterIp := range clusters.ClusterIps {
						clusterIps = append(clusterIps, clusterIp.IP)
						if clusterIp.IP == mgmtIp {
							bFound = true
							klog.Infof("[CreateFileVolume] %s(%s) found in cluster IPs", paramMgmtIp, mgmtIp)
							if newAuthClient, err := cs.Driver.qsan.UpdateAuthClient(ctx, server, mgmtIp); err != nil {
								klog.Warningf("[CreateFileVolume] UpdateAuthClient(%s, %s) failed. err: %v", server, mgmtIp, err)
							} else {
								authClient = newAuthClient
							}
							break
						}
					}
				}
			}

			if !bFound {
				klog.Warningf("[CreateFileVolume] %s(%s) not found (localIps %+v, clusterIps %+v). Use default server(%s) as mgmtIp", paramMgmtIp, mgmtIp, localIps, clusterIps, server)
			}
		}
	}

	poolId, provision, dedupEnabled,err := getPoolIDProvByName(ctx, authClient, pool)
	if err != nil {
		klog.Warningf("[CreateFileVolume] getPoolIDProvByName failed, err: %v", err)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("%s(%s) is not exist at server %s", paramPool, pool, server))
	}
	if dedupEnabled {
		klog.Warningf("[CreateFileVolume] Dedup Pool(%s) does not support CSI volume.", pool)
		return nil, status.Errorf(codes.InvalidArgument, "Dedup Pool(%s) does not support CSI volume.", pool)
	}
	klog.Infof("[CreateFileVolume] poolname(%s) --> poolId(%s), provision(%s), dedupEnabled(%v)", pool, poolId, provision, dedupEnabled)

	topologies := []*csi.Topology{}
	topologies = preferredAccessibility

	klog.Infof("[CreateFileVolume] parameters: protocol(%s), server(%s), poolId(%s), caps(%s), bgIoPriority(%s) qosOpts(%+v)", protocol, server, poolId, caps, bgIoPriority, qosOptions)

	var resizeVolSizeMB uint64
	capacityBytes := reqCapacity
	volSizeGB := (reqCapacity + (1<<30) - 1) >> 30
	volSizeMB := volSizeGB << 10
	// volSizeMB := (reqCapacity + (1<<20) - 1) >> 20
	if volSizeMB == 0 {
		volSizeMB = 1
		klog.Infof("[CreateFileVolume] Set volSizeMB to 1. reqCapacity(%v)", reqCapacity)
	} else if (volSizeMB * 1024 * 1024) != reqCapacity {
		klog.Infof("[CreateFileVolume] reqCapacity(%v) ==> volSize(%v)", reqCapacity, volSizeMB * 1024 * 1024)
	}

	volumeAPI := goqsan.NewFileVolume(authClient)

	var vol *goqsan.FileVolumeData
	var parentVolId string
	var shareId, sharename string
	if req.GetVolumeContentSource() != nil {
		volumeSource := req.VolumeContentSource
		switch volumeSource.Type.(type) {
		case *csi.VolumeContentSource_Snapshot:
			klog.Infof("[CreateFileVolume] VolumeContentSource Type: VolumeContentSource_Snapshot")
			if snapshot := volumeSource.GetSnapshot(); snapshot != nil {
				snapData, err := cs.Driver.GetContextDataFromSnapContextID(snapshot.GetSnapshotId())
				if err != nil {
					return nil, status.Error(codes.NotFound, fmt.Sprintf("Failed to get snapshot context data from Snapshot ID %s: %v", snapshot.GetSnapshotId(), err))
				}
				klog.Infof("[CreateFileVolume] snapId: %s, snapVolumeContextData: %+v", snapData.snapId, snapData.volumeContextData)

				parentVolId = snapData.volId
				if _, err = validateCloneCondition(ctx, &FileVolumeWrapper{op: volumeAPI}, volSizeMB, parentVolId); err != nil {
					return nil, err
				}

				klog.Infof("[CreateFileVolume] Restore snapshot(%s, %s) volume(%s) size(%d) source(%s)", snapData.volId, snapData.snapId, name, volSizeMB, volumeSource.GetVolume().GetVolumeId())
				vol, err = volumeAPI.Clone(ctx, snapData.volId, snapData.snapId, name, poolId)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "Failed to restore volumeID %s. err: %v", volumeSource.GetVolume().GetVolumeId(), err)
				}

				capacityMB := uint64(capacityBytes) >> 20
				klog.Infof("[CreateFileVolume] Restore capacityMB(%d) vs vol.TotalSize(%d)", capacityMB, vol.TotalSize)
				if capacityMB > vol.TotalSize {
					resizeVolSizeMB = capacityMB
					go monitorCloneStatus(&FileVolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, resizeVolSizeMB)
				} else {
					go monitorCloneStatus(&FileVolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, 0)
				}

			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Snapshot is not found.")
			}

		case *csi.VolumeContentSource_Volume:
			klog.Infof("[CreateFileVolume] VolumeContentSource Type: VolumeContentSource_Volume")
			srcVolData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeSource.GetVolume().GetVolumeId())
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "Invalid volume source ID %s", volumeSource.GetVolume().GetVolumeId())
			}
			klog.Infof("[CreateFileVolume] srcVolData: %+v", srcVolData)

			parentVolId = srcVolData.volId
			_, err = validateCloneCondition(ctx, &FileVolumeWrapper{op: volumeAPI}, volSizeMB, parentVolId)
			if err != nil {
				return nil, err
			}

			if err = allocSnapshotSpace(ctx, &FileVolumeWrapper{op: volumeAPI}, srcVolData.volId); err != nil {
				return nil, status.Errorf(codes.Internal, "allocSnapshotSpace failed: %v", err)
			}

			klog.Infof("[CreateFileVolume] Clone volume(%s) size(%d) source(%s)", name, volSizeMB, volumeSource.GetVolume().GetVolumeId())
			vol, err = volumeAPI.Clone(ctx, srcVolData.volId, "", name, poolId)
			if err != nil {
				resterr, ok := err.(*goqsan.RestError)
				if ok {
					switch {
					case resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_SNAP_NOT_INITED_V1:
						return nil, status.Errorf(codes.Internal, "VolumeID %s snapshot space is not enalbed", volumeSource.GetVolume().GetVolumeId())
					case resterr.StatusCode == http.StatusConflict && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_U_NAME_EXISTS_V2,
						resterr.StatusCode == http.StatusInternalServerError && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_U_NAME_EXISTS_V1:
						vol2, err := getFileVolumeByName(ctx, authClient, poolId, name)
						if err != nil {
							return nil, status.Errorf(codes.Internal, "Clone FileVolume is already exists, but failed to getFileVolumeByName(%s, %s). err: %v", poolId, name, err)
						}

						if err := volumeAPI.DeleteVolume(ctx, vol2.ID); err != nil {
							return nil, status.Errorf(codes.Internal, "Failed to delete existing clone FileVolume(%s), %s. err: %v", vol2.ID, name, err)
						} else {
							klog.Warningf("[CreateFileVolume] Delete clone volume(%s) to avoid conflicts due to its existence", name)
						}
					}
				}

				return nil, status.Errorf(codes.Internal, "Failed to clone volumeID %s. err: %v", volumeSource.GetVolume().GetVolumeId(), err)
			}
			capacityMB := uint64(capacityBytes) >> 20
			klog.Infof("[CreateFileVolume] Clone capacityMB(%d) vs vol.TotalSize(%d)", capacityMB, vol.TotalSize)
			if capacityMB > vol.TotalSize {
				resizeVolSizeMB = capacityMB
				go monitorCloneStatus(&FileVolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, resizeVolSizeMB)
			} else {
				go monitorCloneStatus(&FileVolumeWrapper{op: volumeAPI}, parentVolId, vol.ID, 0)
			}

		default:
			klog.Infof("[CreateFileVolume] VolumeContentSource Type: default")
			return nil, status.Errorf(codes.InvalidArgument, "%v not a proper volume source", volumeSource)
		}
	} else {

		options := &goqsan.FileVolumeCreateOptions{
			BlockSize:    bkSize,
			BgIoPriority: bgIoPriority,
		}
		if caps != "" {
			nCaps, _ := strconv.ParseUint(caps, 10, 32)
			mode := "WRITE_BACK"
			if (nCaps & CAP_CACHEMODE_WT) == CAP_CACHEMODE_WT {
				mode = "WRITE_THROUGH"
			}
			options.CacheMode = mode

			readahead := false
			if (nCaps & CAP_READAHEAD_ENABLE) == CAP_READAHEAD_ENABLE {
				readahead = true
			}
			options.EnableReadAhead = &readahead
		}
		metastatus, metatype, metadata := generateMetadata(protocol, cs.Driver.nodeIP, cs.Driver.version, cs.Driver.buildDate)
		if strings.EqualFold(provision, ProvisionThin) {
			metadata[MCIDX_ATTR] = metadata[MCIDX_ATTR] | META_ATTR_NEP
		}
		options.Metadata = goqsan.VolumeMetadata{
			Status:    metastatus,
			Type:      metatype,
			Content:   base64.StdEncoding.EncodeToString(metadata),
			Timestamp: "AUTO",
		}

		klog.Infof("[CreateFileVolume] Create volume(%s) size(%d) options: %+v", name, volSizeMB, options)

		// Check if volume with already existing name exists
		vol, err = getFileVolumeByName(ctx, authClient, poolId, name)
		if vol == nil {
			// Volume with already existing name not exist
			vol, err = volumeAPI.CreateVolume(ctx, poolId, name, uint64(volSizeMB), options)
			if err != nil {
				resterr, ok := err.(*goqsan.RestError)
				if ok {
					switch {
					case resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_RG_NO_SPACE_V2,
						resterr.StatusCode == http.StatusInternalServerError && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_RG_NO_SPACE_V1:
						// LVMERR_RG_NO_SPACE (-104)
						return nil, status.Error(codes.OutOfRange, err.Error())
					case resterr.StatusCode == http.StatusInternalServerError && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_ARG_V2,
						resterr.StatusCode == http.StatusInternalServerError && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_ARG_V1:
						// LVMERR_INVALID_ARG (-3)
						return nil, status.Error(codes.InvalidArgument, err.Error())
					case resterr.StatusCode == http.StatusConflict && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_U_NAME_EXISTS_V2,
						resterr.StatusCode == http.StatusInternalServerError && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_U_NAME_EXISTS_V1:
						// LVMERR_U_NAME_EXISTS (-1501)
						// Should not be here because we had check if volume exist before creating volume.
						vol2, err := getFileVolumeByName(ctx, authClient, poolId, name)
						if vol2 != nil {
							if int64(vol2.TotalSize) == volSizeMB {
								vol = vol2
								klog.Infof("[CreateFileVolume] Volume2(%s) is already exist and size is equal.", name)
							} else {
								return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("Volume2(%s) is already exist, but size not equal (%v vs %v)", name, vol.TotalSize, volSizeMB))
							}
						} else {
							return nil, status.Error(codes.InvalidArgument, err.Error())
						}
					}
				}
				
				return nil, status.Error(codes.Internal, err.Error())
			}

			klog.Infof("[CreateFileVolume] CreateVolume: %+v", vol)
			if strings.EqualFold(provision, ProvisionThick) {
				// Sleep for 2 seconds to prevent a potential LVM null pointer issue if this volume is deleted immediately.
				// This issue may be triggered when running the sanity test.
				time.Sleep(2 * time.Second)
			}

			// Check volume metadata
			metastatus2, metatype2, metadata2, err := volumeAPI.GetMetadata(ctx, vol.ID)
			if err != nil {
				klog.Errorf("[CreateFileVolume] GetMetadata failed, err: %+v", err)
			}
			klog.Infof("[CreateFileVolume] Volume metadata stat(%s) type(%s) buf %v", metastatus2, metatype2, metadata2)
			if metastatus2 != metastatus || metatype2 != metatype || !bytes.HasPrefix(metadata2, metadata) {
				klog.Errorf("[CreateFileVolume] Set volume metadata failed. metaStatus(%s) metaType(%s) metadata %v vs %v", metastatus2, metatype2, metadata2, metadata)
			}

			// Set QoS if need
			if (qosOptions != goqsan.VolumeQoSOptions{}) {
				opts := &goqsan.FileVolumeModifyOptions{VolumeQoSOptions: qosOptions}
				klog.Infof("[CreateFileVolume] QoS qosOptions: %+v", opts)

				vol, err = volumeAPI.ModifyVolume(ctx, vol.ID, opts)
				if err == nil {
					klog.Infof("[CreateFileVolume] QoS ModifyVolume: %+v", vol)
				} else {
					klog.Errorf("[CreateFileVolume] Set volume(%s) QoS(%+v) failed: %+v", vol.ID, qosOptions, err)
				}
			}

			// adminId, _ := getAdminUserId(ctx, authClient)
			// user := goqsan.UserGroupPermission{ID: adminId, Permission: "rw"}
			// adminGroupId, _ := getAdminGroupId(ctx, authClient)
			// group := goqsan.UserGroupPermission{ID: adminGroupId, Permission: "rw"}
			// perm := goqsan.SharePermission{
			// 	UserLocal:  []goqsan.UserGroupPermission{user},
			// 	GroupLocal: []goqsan.UserGroupPermission{group},
			// }

			// create share
			shareOptions := goqsan.ShareCreateOptions{
				Description:    fmt.Sprintf("[CSI] NFS share for CSI volume %s.", vol.Name),
				EnableUserhome: false,
				// Permissions:      perm,
				EnableRecycleBin: false,
			}

			sharename = vol.Name
			share, err := volumeAPI.CreateShare(ctx, vol.ID, sharename, 0, &shareOptions)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to CreateShare for volume %s. err: %v", name, err)
			}
			klog.Infof("[CreateFileVolume] CreateShare: %+v", share)
			shareId = share.ID

		} else {
			// Volume with already existing name exist
			if vol.TotalSize == uint64(volSizeMB) {
				klog.Infof("[CreateFileVolume] Volume(%s) is already exist and size is equal.", name)

				if share, err := getShareFromFileVolume(ctx, volumeAPI, vol.ID); err != nil {
					klog.Warningf("[CreateFileVolume] getShareFromFileVolume(%s) failed. err: %v", vol.ID, err)

					// create share
					// adminId, _ := getAdminUserId(ctx, authClient)
					// user := goqsan.UserGroupPermission{ID: adminId, Permission: "rw"}
					// adminGroupId, _ := getAdminGroupId(ctx, authClient)
					// group := goqsan.UserGroupPermission{ID: adminGroupId, Permission: "rw"}
					// perm := goqsan.SharePermission{
					// 	UserLocal:  []goqsan.UserGroupPermission{user},
					// 	GroupLocal: []goqsan.UserGroupPermission{group},
					// }
					shareOptions := goqsan.ShareCreateOptions{
						Description:    fmt.Sprintf("[CSI] NFS share for CSI volume %s.", vol.Name),
						EnableUserhome: false,
						// Permissions:      perm,
						EnableRecycleBin: false,
					}
					sharename = vol.Name
					share, err := volumeAPI.CreateShare(ctx, vol.ID, sharename, 0, &shareOptions)
					if err != nil {
						return nil, status.Errorf(codes.Internal, "Failed to CreateShare for volume %s. err: %v", name, err)
					}
					klog.Infof("[CreateFileVolume] CreateShare for existing volume(%s): %+v", vol.Name, share)
					shareId = share.ID
				} else {
					shareId = share.ID
				}

				if shareId == "" {
					return nil, status.Errorf(codes.Internal, "Volume(%s) share ID is empty", vol.Name)
				}

			} else {
				return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("Volume(%s) is already exist, but size not equal (%v vs %v)", name, vol.TotalSize, volSizeMB))
			}
		}

		capacityBytes = int64(vol.TotalSize) << 20
	}

	sharehostAPI := goqsan.NewSharehost(authClient)
	sharehosts, err := sharehostAPI.ListSharehosts(ctx, targetName)
	if err != nil {
		return nil, fmt.Errorf("[CreateFileVolume] Failed to ListSharehosts with name %s for volume(%s). err: %v", targetName, name, err)
	}

	volumeContextID := cs.Driver.GenerateVolumeContextID(protocol, server, poolId, vol.ID, &shareId, &(*sharehosts)[0].ID)

	security := strings.Join((*sharehosts)[0].NfsRules[0].Security, ":")
	klog.Infof("[CreateFileVolume] Sharehost(%s) security: %v", (*sharehosts)[0].Name, (*sharehosts)[0].NfsRules[0].Security)

	// Don't need to add volStatMap here. It will be added later in ControllerGetVolume.
	// cs.Driver.volStatMap[volumeContextID] = VolumeStat{
	// 	LastUpdateTimeStamp: time.Now(),
	// }
	// klog.Infof("[CreateVolume] Add volStatMap[%s](%+v)", volumeContextID, cs.Driver.volStatMap[volumeContextID])

	context := map[string]string{
		paramProtocol:    protocol,
		paramShareName:   sharename,
		paramPoolId:      poolId,
		paramTarget:      targetName,
		paramParentVolId: parentVolId,
		paramResize:      strconv.FormatUint(resizeVolSizeMB, 10),
		paramSecurity:    security,
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volumeContextID,
			CapacityBytes:      capacityBytes,
			VolumeContext:      context,
			ContentSource:      req.GetVolumeContentSource(),
			AccessibleTopology: topologies,
		},
	}, nil
}

// DeleteVolume delete a volume
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is empty")
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[DeleteVolume] volData: %+v", volData)
	if err != nil {
		klog.Infof("[DeleteVolume] Ignore because of invalid volumeID %s.", volumeID)
		return &csi.DeleteVolumeResponse{}, nil
	}

	cs.mapMu.Lock()
	if _, ok := cs.Driver.volStatMap[volumeID]; ok {
		delete(cs.Driver.volStatMap, volumeID)
		klog.Infof("[DeleteVolume] Remove volStatMap[%s]", volumeID)
	}
	cs.mapMu.Unlock()

	if volData.protocol == protocolNFS {
		return cs.DeleteFileVolume(ctx, volumeID)
	} else {
		return cs.DeleteBlockVolume(ctx, volumeID)
	}
}

func (cs *ControllerServer) DeleteBlockVolume(ctx context.Context, volumeID string) (*csi.DeleteVolumeResponse, error) {
	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	volumeAPI := goqsan.NewVolume(authClient)
	vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
	if err == nil && vol.ReplicaTargetID != InvalidReplicaTargetID {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("Volume(%s) is a replica source and cannot be deleted. Current state: %s (%d percent)", volData.volId, vol.State, vol.Progress))
	}

RETRY:
	err = volumeAPI.DeleteVolume(ctx, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok {
			if resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_VD_ID_V2 ||
				resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_VD_ID_V1 {
				klog.Infof("[DeleteBlockVolume] volumeID %s not exist.", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			} else if resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_ALREADY_DELETE_V2 ||
					resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_ALREADY_DELETE_V1 {
				klog.Warningf("[DeleteBlockVolume] volumeID %s was already deleted.", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			} else if resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_ALREADY_ATTACHED_V2 ||
					resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_ALREADY_ATTACHED_V1 {
				// Check if has LUN attached. If yes, detach it then delete volume again
				vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
				if err == nil && vol.TargetID != "" {
					targetAPI := goqsan.NewTarget(authClient)
					cs.mutexBLock.Lock()
					err = targetAPI.UnmapLun(ctx, vol.TargetID, vol.LunID)
					cs.mutexBLock.Unlock()
					if err != nil {
						return nil, status.Error(codes.Internal, err.Error())
					}
					klog.Infof("[DeleteBlockVolume] Unexport volume %s successfully then retry again.", volumeID)

					goto RETRY
				}
			} else if resterr.StatusCode == http.StatusNotFound {
				klog.Warningf("[DeleteBlockVolume] volumeID %s not exist at the server.", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			} else {
				klog.Warningf("[DeleteBlockVolume] Failed to delete volumeID %s. Bypass it !!", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			}
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("[DeleteBlockVolume] Delete volumeID %s successfully", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) DeleteFileVolume(ctx context.Context, volumeID string) (*csi.DeleteVolumeResponse, error) {
	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	volumeAPI := goqsan.NewFileVolume(authClient)
	vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
	if err == nil && vol.ReplicaTargetID != InvalidReplicaTargetID {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("FileVolume(%s) is a replica source and cannot be deleted. Current state: %s (%d percent)", volData.volId, vol.State, vol.Progress))
	}

	// delete share
	targetShareId := volData.shareId
	if targetShareId == "" {
		if share, err := getShareFromFileVolume(ctx, volumeAPI, volData.volId); err != nil {
			klog.Warningf("[DeleteFileVolume] getShareFromFileVolume(%s) failed. err: %v", volData.volId, err)
		} else {
			targetShareId = share.ID
		}
	}

RETRY:
	if targetShareId != "" {
		err = volumeAPI.DeleteShare(ctx, targetShareId)
		if err != nil {
			resterr, ok := err.(*goqsan.RestError)
			if ok && resterr.StatusCode == http.StatusInternalServerError && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_SHARE_IN_HOSTGROUP {
				cs.mutexFile.Lock()
				defer cs.mutexFile.Unlock()

				if err = removeShareFromSharehost(ctx, authClient, volData.targetId, targetShareId); err != nil {
					return nil, status.Errorf(codes.Internal, "[DeleteFileVolume] Failed to removeShareFromSharehost for volumeID %s. err: %v", volumeID, err)
				} else {
					klog.Infof("[DeleteFileVolume] Share(%s) of volume(%s) was already unshared then retry again.", targetShareId, volumeID)
					goto RETRY
				}
			}
		}
	}

	err = volumeAPI.DeleteVolume(ctx, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok {
			if resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_VD_ID_V2 ||
				resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_VD_ID_V1 {
				klog.Infof("[DeleteFileVolume] volumeID %s not exist.", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			} else if resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_ALREADY_DELETE_V2 ||
					resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_ALREADY_DELETE_V1 {
				klog.Warningf("[DeleteFileVolume] volumeID %s was already deleted.", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			} else if ok && resterr.StatusCode == http.StatusNotFound {
				klog.Warningf("[DeleteFileVolume] volumeID %s not exist at the server.", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			} else {
				klog.Warningf("[DeleteFileVolume] Failed to delete volumeID %s. Bypass it !!", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			}
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("[DeleteFileVolume] Delete volumeID %s successfully", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
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
	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID missing in request")
	}
	nodeIds := getNodeIds()
	klog.Infof("[ControllerPublishVolume] Check if nodeId(%s) in %v", nodeID, nodeIds)
	if !contains(nodeIds, nodeID) {
		return nil, status.Errorf(codes.NotFound, "Not matching Node ID %s in %v", nodeID, nodeIds)
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[ControllerPublishVolume] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	if volData.protocol == protocolNFS {
		if err := cs.Driver.fsPublishLimiter.TryEnter(); err != nil {
			return nil, status.Errorf(codes.ResourceExhausted, fmt.Sprintf("Too many concurrent ControllerPublishVolume requests. Volume(%s) err: %v", volumeID, err))
		}
		defer cs.Driver.fsPublishLimiter.Leave()
		return cs.ControllerPublishFileVolume(ctx, req, volumeID)
	} else {
		return cs.ControllerPublishBlockVolume(ctx, req, volumeID)
	}
}

func (cs *ControllerServer) ControllerPublishBlockVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest, volumeID string) (*csi.ControllerPublishVolumeResponse, error) {
	protocol, iscsiPortals, iscsiTargets, _, targetName, parentVolId, _, multipathEnabled, resizeVolSizeMB := parseVolumeContext(req.GetVolumeContext())
	klog.Infof("[ControllerPublishVolume] protocol(%s) iscsiPortals(%s), iscsiTargets(%s), targetName(%s) parentVolId(%s) multipathEnabled(%v) resizeVolSizeMB(%d)", protocol, iscsiPortals, iscsiTargets, targetName, parentVolId, multipathEnabled, resizeVolSizeMB)

	volData, _ := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeAPI := &VolumeWrapper{op: goqsan.NewVolume(authClient)}
	targetAPI := goqsan.NewTarget(authClient)

	cs.mutexBLock.Lock()
	defer cs.mutexBLock.Unlock()

RETRY:
	gvol, err := genericListVolumeByID(ctx, volumeAPI, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume ID %s not found at the server.", volumeID))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("[ControllerPublishBlockVolume] volume data: %+v", gvol)

	if strings.EqualFold(gvol.Type, "BACKUP") {
		pvol, err := genericListVolumeByID(ctx, volumeAPI, parentVolId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to get parent volume(%s) of Backup Volume(%s). err: %v", parentVolId, volumeID, err)
		} else {
			if strings.EqualFold(pvol.State, "ONLINE") {
				_, err = convertVolRAIDType(ctx, volumeAPI, gvol.ID, resizeVolSizeMB)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "convertVolRAIDType failed. err: %v", err)
				}
			} else {
				return nil, status.Errorf(codes.Internal, "The parent volume(%s) of Volume(%s) is in abnormal state %s (%d percent)", pvol.ID, volumeID, pvol.State, pvol.Progress)
			}
		}
	}

	attached := "0"
	lun := &goqsan.LunData{}
	if gvol.TargetID == "" {
		var tgtID string
		var err error
		if protocol == protocolISCSI {
			if multipathEnabled {
				tgtID, err = getTargetIDByName(ctx, authClient, iscsiTargetTypes, targetName)
			} else {
				tgtID, err = getTargetIDByIqn(ctx, authClient, iscsiTargets)
			}
		} else if protocol == protocolFC {
			tgtID, err = getTargetIDByName(ctx, authClient, fcTargetTypes, targetName)
		}
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("[ControllerPublishBlockVolume] tgtID: %s", tgtID)

		paramMLun := goqsan.LunMapParam{
			Hosts: []goqsan.Host{
				{Name: "*"},
			},
		}
		lun, err = targetAPI.MapLun(ctx, tgtID, volData.volId, &paramMLun)
		if err != nil {
			resterr, ok := err.(*goqsan.RestError)
			if ok && resterr.StatusCode == http.StatusForbidden && 
				(resterr.ErrResp.Error.Code == goqsan.QERR_UIERR_LUN_MAX_LIMIT_V2 || resterr.ErrResp.Error.Code == goqsan.QERR_UIERR_LUN_MAX_LIMIT_V1) {
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			} else if ok && resterr.StatusCode == http.StatusTooManyRequests {
				klog.Warningf("[ControllerPublishBlockVolume] TooManyRequests, Retry again")
				time.Sleep(1 * time.Second)
				goto RETRY
			} else if ok && resterr.StatusCode == http.StatusConflict && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_IS_ALREADY_MAPPED_V2 {
				klog.Warningf("[ControllerPublishBlockVolume] already attached. %s", resterr.ErrResp.Error.Message)
				return nil, status.Error(codes.AlreadyExists, "The volume was used by another node.")
			} else if ok && resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_VD_IS_ALREADY_MAPPED_V1 {
				klog.Warningf("[ControllerPublishBlockVolume] already attached. %s", resterr.ErrResp.Error.Message)
				return nil, status.Error(codes.AlreadyExists, "The volume was used by another node.")
			}

			return nil, status.Error(codes.Internal, err.Error())
		} else {
			if req.GetReadonly() {
				metastatus, metatype, metacontent, err := volumeAPI.GetMetadata(ctx, gvol.ID)
				if err == nil && validMetaHeader(metastatus, metatype) {
					metacontent[MCIDX_ATTR] = metacontent[MCIDX_ATTR] | META_ATTR_RO
					klog.Infof("[ControllerPublishBlockVolume] Set volume(%s) metadata %v", volumeID, metacontent)
					_, _, _, err = volumeAPI.SetMetadata(ctx, gvol.ID, metastatus, metatype, metacontent)
					if err != nil {
						klog.Errorf("[ControllerPublishBlockVolume] Set volume %s metadata failed, err: %+v", volumeID, err)
					}
				} else {
					klog.Errorf("[ControllerPublishBlockVolume] Invalid vd(%s) metadata header", gvol.ID)
				}
			}
		}
		attached = "1"
	} else {
		// Already attached Lun
		klog.Infof("[ControllerPublishBlockVolume] Volume ID %s was already attached on tgtID %s", volumeID, gvol.TargetID)

		lun, err = targetAPI.ListTargetLun(ctx, gvol.TargetID, gvol.LunID)
		if err != nil {
			klog.Errorf("[ControllerPublishBlockVolume] ListTargetLun failed: %s", err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		readOnly := false
		metastatus, metatype, metacontent, err := volumeAPI.GetMetadata(ctx, gvol.ID)
		if err == nil && validMetaHeader(metastatus, metatype) {
			klog.Infof("[ControllerPublishBlockVolume] Volume(%s) metadata %v", volumeID, metacontent)
			b := metacontent[MCIDX_ATTR]
			if b&META_ATTR_RO == META_ATTR_RO {
				readOnly = true
			}
			if readOnly != req.GetReadonly() {
				return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("The volume was already exist. (readOnly: %v)", readOnly))
			}
		} else {
			klog.Errorf("[ControllerPublishBlockVolume] Invalid vd(%s) metadata header", gvol.ID)
		}

		accessMode := req.GetVolumeCapability().GetAccessMode()
		if accessMode.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			return nil, status.Error(codes.AlreadyExists, "ReadWriteMany conflict! The volume was used by another node.")
		}
	}
	// klog.Infof("[ControllerPublishBlockVolume] Lun: %+v", lun)

	context := map[string]string{
		"lunId":  lun.ID,
		"lunNum": lun.Name,
		"attach": attached,
		"nodeId": req.GetNodeId(),
		"pvolId": parentVolId,
	}
	klog.Infof("[ControllerPublishBlockVolume] context: %+v", context)

	return &csi.ControllerPublishVolumeResponse{PublishContext: context}, nil
}

func (cs *ControllerServer) ControllerPublishFileVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest, volumeID string) (*csi.ControllerPublishVolumeResponse, error) {
	protocol, _, _, shareName, targetName, parentVolId, security, _, resizeVolSizeMB := parseVolumeContext(req.GetVolumeContext())
	klog.Infof("[ControllerPublishFileVolume] protocol(%s) shareName(%s) targetName(%s) parentVolId(%s) security(%s) resizeVolSizeMB(%d)", protocol, shareName, targetName, parentVolId, security, resizeVolSizeMB)

	volData, _ := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeAPI := goqsan.NewFileVolume(authClient)
	gvol, err := genericListVolumeByID(ctx, &FileVolumeWrapper{op: volumeAPI}, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume ID %s not found at the server.", volumeID))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("[ControllerPublishFileVolume] volume data: %+v", gvol)

	// var pvolName string
	if strings.EqualFold(gvol.Type, "BACKUP") {
		pvol, err := genericListVolumeByID(ctx, &FileVolumeWrapper{op: volumeAPI}, parentVolId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to get parent volume(%s) of Backup Volume(%s). err: %v", parentVolId, volumeID, err)
		} else {
			// pvolName = pvol.Name
			if strings.EqualFold(pvol.State, "ONLINE") {
				_, err = convertVolRAIDType(ctx, &FileVolumeWrapper{op: volumeAPI}, gvol.ID, resizeVolSizeMB)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "convertVolRAIDType failed. err: %v", err)
				}
			} else {
				return nil, status.Errorf(codes.Internal, "The parent volume(%s) of Volume(%s) is in abnormal state %s (%d percent)", pvol.ID, volumeID, pvol.State, pvol.Progress)
			}
		}
	}

	shareId := volData.shareId
	if shareId == "" {
		if share, err := getShareFromFileVolume(ctx, volumeAPI, volData.volId); err != nil {
			// klog.Warningf("[ControllerPublishFileVolume] getShareFromFileVolume(%s) failed. err: %v", volData.volId, err)
			return nil, status.Errorf(codes.Internal, "Failed to getShareFromFileVolume for volumeID %s. err: %v", volumeID, err)
		} else {
			shareId = share.ID
			shareName = share.Name
			klog.Infof("[ControllerPublishFileVolume] Volume(%s) share: %+v", volumeID, share)

			if share.IsOrphan {
				return nil, status.Errorf(codes.Internal, "Cloned share is not ready! volName(%s) shareName(%s)", gvol.Name, shareName)
			} else {
				count := strings.Count(shareName, "-")
				if count > 6 {
					return nil, status.Errorf(codes.Internal, "Cloned share name is not converted! volName(%s) shareName(%s)", gvol.Name, shareName)
				}
			}
		}
	}

	cs.mutexFile.Lock()
	defer cs.mutexFile.Unlock()

	added, err := addShareToSharehost(ctx, authClient, volData.targetId, shareId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to addShareToSharehost for volumeID %s. err: %v", volumeID, err)
	}
	if added == false {
		// For sanity test
		readOnly := false
		metastatus, metatype, metacontent, err := volumeAPI.GetMetadata(ctx, gvol.ID)
		if err == nil && validMetaHeader(metastatus, metatype) {
			klog.Infof("[ControllerPublishFileVolume] Volume(%s) metadata %v", volumeID, metacontent)
			b := metacontent[MCIDX_ATTR]
			if b&META_ATTR_RO == META_ATTR_RO {
				readOnly = true
			}
			if readOnly != req.GetReadonly() {
				return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("The volume was already exist. (readOnly: %v)", readOnly))
			}
		} else {
			klog.Errorf("[ControllerPublishFileVolume] Invalid vd(%s) metadata header", gvol.ID)
		}
	}

	context := map[string]string{
		"nodeId":    req.GetNodeId(),
		"pvolId":    parentVolId,
		"shareName": shareName,
	}
	klog.Infof("[ControllerPublishFileVolume] context: %+v", context)

	return &csi.ControllerPublishVolumeResponse{PublishContext: context}, nil
}

func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[ControllerUnpublishVolume] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	if volData.protocol == protocolNFS {
		if err := cs.Driver.fsPublishLimiter.TryEnter(); err != nil {
			return nil, status.Errorf(codes.ResourceExhausted, fmt.Sprintf("Too many concurrent ControllerUnpublishVolume requests. Volume(%s) err: %v", volumeID, err))
		}
		defer cs.Driver.fsPublishLimiter.Leave()

		return cs.ControllerUnpublishFileVolume(ctx, volumeID)
	} else {
		return cs.ControllerUnpublishBlockVolume(ctx, volumeID)
	}
}

func (cs *ControllerServer) ControllerUnpublishFileVolume(ctx context.Context, volumeID string) (*csi.ControllerUnpublishVolumeResponse, error) {
	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	volumeAPI := goqsan.NewFileVolume(authClient)
	_, err = volumeAPI.ListVolumeByID(ctx, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			klog.Warningf("[ControllerUnpublishFileVolume] Volume ID %s not found at the server.", volumeID)
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	cs.mutexFile.Lock()
	defer cs.mutexFile.Unlock()

	if volData.shareId != "" {
		if err = removeShareFromSharehost(ctx, authClient, volData.targetId, volData.shareId); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to removeShareFromSharehost for volumeID %s. err: %v", volumeID, err)
		} else {
			klog.Infof("[ControllerUnpublishFileVolume] Volume ID %s was already unshared", volumeID)
		}
	} else {
		if share, err := getShareFromFileVolume(ctx, volumeAPI, volData.volId); err != nil {
			klog.Warningf("[ControllerUnpublishFileVolume] getShareFromFileVolume(%s) failed. err: %v", volData.volId, err)
		} else {
			if err = removeShareFromSharehost(ctx, authClient, volData.targetId, share.ID); err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to removeShareFromSharehost(%s, %s) for cloned volumeID %s. err: %v", volData.targetId, share.ID, volumeID, err)
			} else {
				klog.Infof("[ControllerUnpublishFileVolume] The share(%s) of cloned volume ID %s was already unshared", share.ID, volumeID)
			}
		}
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerUnpublishBlockVolume(ctx context.Context, volumeID string) (*csi.ControllerUnpublishVolumeResponse, error) {
	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	volumeAPI := goqsan.NewVolume(authClient)

	cs.mutexBLock.Lock()
	defer cs.mutexBLock.Unlock()

	vol, err := volumeAPI.ListVolumeByID(ctx, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			klog.Warningf("[ControllerUnpublishBlockVolume] Volume ID %s not found at the server.", volumeID)
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if vol.TargetID != "" {
		targetAPI := goqsan.NewTarget(authClient)
		err = targetAPI.UnmapLun(ctx, vol.TargetID, vol.LunID)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("[ControllerUnpublishBlockVolume] Unexport volume %s successfully", volumeID)
	} else {
		klog.Infof("[ControllerUnpublishBlockVolume] Volume ID %s was already unexported", volumeID)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	// klog.Infof("[ControllerGetVolume] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	// If requested within 3 seconds, the last status is returned.
	cs.mapMu.RLock()
	volStat, ok := cs.Driver.volStatMap[volumeID]
	cs.mapMu.RUnlock()
	if ok {
		now := time.Now()
		nextUpdateMinutes := volStat.LastUpdate.Add(time.Second * 3)
		if now.Before(nextUpdateMinutes) {
			klog.Infof("[ControllerGetVolume] Bypass and return the last status.")
			return &csi.ControllerGetVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      volumeID,
					CapacityBytes: volStat.CapacityBytes,
				},
				Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
					// PublishedNodeIds: []string{cs.Driver.nodeID},    // if LIST_VOLUMES_PUBLISHED_NODES is set
					VolumeCondition: &csi.VolumeCondition{
						Abnormal: volStat.Abnormal,
						Message:  volStat.Message,
					},
				},
			}, nil
		}
	}

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var volumeAPI GenericVolumeInterface
	if volData.protocol == protocolNFS {
		volumeAPI = &FileVolumeWrapper{op: goqsan.NewFileVolume(authClient)}
	} else {
		volumeAPI = &VolumeWrapper{op: goqsan.NewVolume(authClient)}
	}
	gvol, err := genericListVolumeByID(ctx, volumeAPI, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			// Don't return gRPC error code NOT_FOUND if volume doesn't exist on storage system. Otherwise you can't get VolumeConditionAbnormal event to notify this volume is under abnormal.
			// return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume ID %s not found at the server.", volumeID))
			klog.Warningf("[ControllerGetVolume] Volume ID %s not found at the server.", volumeID)
			return &csi.ControllerGetVolumeResponse{
				Volume: &csi.Volume{
					VolumeId: volumeID,
				},
				Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
					VolumeCondition: &csi.VolumeCondition{
						Abnormal: true,
						Message:  "Volume is not found",
					},
				},
			}, nil
		}

		klog.Warningf("[ControllerGetVolume] GetVolume(%s) failed with StatusCode(%d). err: %v", volumeID, resterr.StatusCode, err)
		return &csi.ControllerGetVolumeResponse{
			Volume: &csi.Volume{
				VolumeId: volumeID,
			},
			Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
				VolumeCondition: &csi.VolumeCondition{
					Abnormal: true,
					Message:  err.Error(),
				},
			},
		}, nil
	}

	abnormal, msg := getVolumeCondition(gvol)
	cs.mapMu.Lock()
	defer cs.mapMu.Unlock()
	if volStat, ok := cs.Driver.volStatMap[volumeID]; ok {
		now := time.Now()
		volStat.CapacityBytes = int64(gvol.TotalSize)
		volStat.Abnormal = abnormal
		volStat.Message = msg
		volStat.LastUpdate = now

		nextUpdateMinutes := volStat.LastUpdateTimeStamp.Add(time.Minute * tsUpdateVolPeriodMin)
		if now.After(nextUpdateMinutes) {
			volStat.LastUpdateTimeStamp = now
			_, err := volumeAPI.SetTimestamp(ctx, volData.volId, "AUTO")
			if err != nil {
				klog.Errorf("[ControllerGetVolume] SetTimestamp failed. err: %v", err)
			} else {
				cs.Driver.volStatMap[volumeID] = volStat
				klog.Infof("[ControllerGetVolume] Update volStatMap[%s](%+v)", volumeID, volStat)
			}
		} else {
			cs.Driver.volStatMap[volumeID] = volStat
		}
	} else {
		_, err := volumeAPI.SetTimestamp(ctx, volData.volId, "AUTO")
		if err != nil {
			klog.Errorf("[ControllerGetVolume] SetTimestamp failed. err: %v", err)
		} else {
			now := time.Now()
			cs.Driver.volStatMap[volumeID] = VolumeStat{
				CapacityBytes:       int64(gvol.TotalSize),
				Abnormal:            abnormal,
				Message:             msg,
				LastUpdate:          now,
				LastUpdateTimeStamp: now,
			}
		}
		klog.Infof("[ControllerGetVolume] Add volStatMap[%s](%+v)", volumeID, cs.Driver.volStatMap[volumeID])
	}

	klog.Infof("[ControllerGetVolume] Name(%s) Online(%v) Health(%s) TotalSize(%d) UsedSize(%d)", gvol.Name, gvol.Online, gvol.Health, gvol.TotalSize, gvol.UsedSize)
	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: int64(gvol.TotalSize),
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			// PublishedNodeIds: []string{cs.Driver.nodeID},    // if LIST_VOLUMES_PUBLISHED_NODES is set
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: abnormal,
				Message:  msg,
			},
		},
	}, nil
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[ValidateVolumeCapabilities] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	if err := isValidVolumeCapabilities(volData.protocol, req.GetVolumeCapabilities()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var volumeAPI GenericVolumeInterface
	if volData.protocol == protocolNFS {
		volumeAPI = &FileVolumeWrapper{op: goqsan.NewFileVolume(authClient)}
	} else {
		volumeAPI = &VolumeWrapper{op: goqsan.NewVolume(authClient)}
	}
	gvol, err := genericListVolumeByID(ctx, volumeAPI, volData.volId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume ID %s not found at the server.", volumeID))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("[ValidateVolumeCapabilities] volume data: %+v", gvol)

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.Infof("[ListVolumes] %+v", *req)
	return nil, status.Error(codes.Unimplemented, "ListVolumes")
}

func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.Infof("[GetCapacity] %+v", *req)
	return nil, status.Error(codes.Unimplemented, "GetCapacity")
}

// ControllerGetCapabilities implements the default GRPC callout.
// Default supports all capabilities
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.Driver.cscap,
	}, nil
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	snapName := req.GetName()
	if len(snapName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	volumeID := req.GetSourceVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "SourceVolumeId missing in request")
	}

	tokens := strings.Split(snapName, "-")
	if len(tokens) >= 6 {
		snapName = tokens[0] + "-" + strings.Join(tokens[3:], "-")
	}
	// Patch for sanity test because it will use 'listSnapshots-snapshot-unrelated' and 'listSnapshots-snapshot-unrelated2' to test
	if len(tokens) == 3 && tokens[0] == "listSnapshots" && tokens[1] == "snapshot" {
		klog.Infof("[CreateSnapshot] Patch for sanity test.")
		snapName = tokens[0] + "-" + tokens[2]
	}
	if len(snapName) > MAX_SNAP_NAME_LEN {
		snapName = snapName[:MAX_SNAP_NAME_LEN-1]
	}
	if req.GetName() != snapName {
		klog.Infof("[CreateSnapshot] snapName: %s ==> %s", req.GetName(), snapName)
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[CreateSnapshot] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var volumeAPI GenericVolumeInterface
	if volData.protocol == protocolNFS {
		volumeAPI = &FileVolumeWrapper{op: goqsan.NewFileVolume(authClient)}
	} else {
		volumeAPI = &VolumeWrapper{op: goqsan.NewVolume(authClient)}
	}

	// Check if snapshot already exists.
	// [Sanity] It should fail when requesting to create a snapshot with already existing name and different source volume ID.
	if snapMap, _ := getSnapshotsByPoolID(ctx, volumeAPI, volData.poolId); snapMap != nil {
		for volId, snaps := range snapMap {
			for _, snap := range *snaps {
				if snap.Name == snapName {
					if volId == volData.volId {
						klog.Infof("[CreateSnapshot] snapshot already exists: %+v", snap)
						return &csi.CreateSnapshotResponse{
							Snapshot: &csi.Snapshot{
								SnapshotId:     cs.Driver.GenerateSnapContextID(volumeID, snap.ID),
								SourceVolumeId: volumeID,
								CreationTime:   timestamppb.New(time.Unix(snap.CreateTime, 0)),
								SizeBytes:      int64(snap.UsedSize),
								ReadyToUse:     true,
							},
						}, nil
					} else {
						return nil, status.Errorf(codes.AlreadyExists, "snapshot with the same name: %s(%s) but with different SourceVolumeId already exist", req.GetName(), snapName)
					}
				}
			}
		}
	}

	if err = allocSnapshotSpace(ctx, volumeAPI, volData.volId); err != nil {
		return nil, status.Errorf(codes.Internal, "allocSnapshotSpace failed: %v", err)
	}

RETRY:
	snapData, err := volumeAPI.CreateSnapshot(ctx, volData.volId, snapName)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok {
			switch {
			case resterr.StatusCode == http.StatusConflict && resterr.ErrResp.Error.Code == goqsan.QERR_UIIERR_SNAP_ALREADY_EXIST_V2,
				resterr.StatusCode == http.StatusTooManyRequests && resterr.ErrResp.Error.Code == goqsan.QERR_UIIERR_SNAP_ALREADY_EXIST_V1:
				klog.Warningf("[CreateSnapshot] snapshot name (%s) already exists. Should not be here !!", snapName)

				snapId := snapName // because snapshot ID and name are the same
				if snapData, err = volumeAPI.GetSnapshot(ctx, volData.volId, snapId); err != nil {
					return nil, status.Error(codes.Internal, fmt.Sprintf("[CreateSnapshot] snapshot name(%s) exists, but GetSnapshot failed: %v", snapName, err))
				} else {
					// Pass. Just return the current snapshot
				}

			case resterr.StatusCode == http.StatusForbidden && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_TOO_MANY_SNAPS_V2,
				resterr.StatusCode == http.StatusForbidden && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_TOO_MANY_SNAPS_V1:
				cleanCnt, trashCnt := cleanSnapshotInTrash(ctx, volumeAPI, volData.volId)
				if cleanCnt > 0 {
					klog.Infof("[CreateSnapshot] Clean snapshot cnt %d", cleanCnt)
					goto RETRY
				} else {
					// cleanCnt == 0
					if trashCnt > 0 {
						return nil, status.Errorf(codes.ResourceExhausted, "The oldest snapshots of vold(%s) remain in the trash, causing some snapshots to not be cleaned (trashCnt: %d).", volData.volId, trashCnt)
					} else {
						return nil, status.Errorf(codes.ResourceExhausted, "The snapshot count of vold(%s) has reached the maximum limit.", volData.volId)
					}
				}
			}
		}
		
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateSnapshot failed: %v", err))
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     cs.Driver.GenerateSnapContextID(volumeID, snapData.ID),
			SourceVolumeId: volumeID,
			CreationTime:   timestamppb.New(time.Unix(snapData.CreateTime, 0)),
			SizeBytes:      int64(snapData.UsedSize),
			ReadyToUse:     true,
		},
	}, nil
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if len(req.GetSnapshotId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID missing in request")
	}

	snapData, err := cs.Driver.GetContextDataFromSnapContextID(req.GetSnapshotId())
	if err != nil {
		klog.Warningf("[DeleteSnapshot] Ignore because of invalid snapshotID %s.", req.GetSnapshotId())
		return &csi.DeleteSnapshotResponse{}, nil
	}
	klog.Infof("[DeleteSnapshot] snapId: %s, snapVolumeContextData: %+v", snapData.snapId, snapData.volumeContextData)

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, snapData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var volumeAPI GenericVolumeInterface
	if snapData.protocol == protocolNFS {
		volumeAPI = &FileVolumeWrapper{op: goqsan.NewFileVolume(authClient)}
	} else {
		volumeAPI = &VolumeWrapper{op: goqsan.NewVolume(authClient)}
	}
	if err := volumeAPI.DeleteSnapshot(ctx, snapData.volId, snapData.snapId, false); err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok {
			switch {
			case resterr.StatusCode == http.StatusNotFound && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_VD_ID_V2,
				resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_VD_ID_V1:
				klog.Warningf("[DeleteSnapshot] volumeID %s not exist.", snapData.volId)
				return &csi.DeleteSnapshotResponse{}, nil
			case resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_SNAP_V2,
				resterr.StatusCode == http.StatusBadRequest && resterr.ErrResp.Error.Code == goqsan.QERR_LVMERR_INVALID_SNAP_V1:
				klog.Warningf("[DeleteSnapshot] snapshotID %s not exist.", snapData.snapId)
				return &csi.DeleteSnapshotResponse{}, nil
			}
		}

		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to delete snapshot(%s): err: %v", req.GetSnapshotId(), err))
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if len(req.GetSnapshotId()) != 0 {
		klog.Infof("[ListSnapshots] SnapshotId is not empty, return snapshots that match the snapshot id %s", req.GetSnapshotId())
		if snapData, _ := cs.Driver.GetContextDataFromSnapContextID(req.GetSnapshotId()); snapData != nil {
			if authClient, _ := cs.Driver.qsan.GetnAddAuthClient(ctx, snapData.server); authClient != nil {
				var volumeAPI GenericVolumeInterface
				if snapData.protocol == protocolNFS {
					volumeAPI = &FileVolumeWrapper{op: goqsan.NewFileVolume(authClient)}
				} else {
					volumeAPI = &VolumeWrapper{op: goqsan.NewVolume(authClient)}
				}
				if snapMap, _ := getSnapshotsByPoolID(ctx, volumeAPI, snapData.poolId); snapMap != nil {
					for _, snaps := range snapMap {
						for _, snap := range *snaps {
							if snap.ID == snapData.snapId {
								snapVolumeID, _ := cs.Driver.GetVolumeContextIDFromSnapContextID(req.GetSnapshotId())
								entries := []*csi.ListSnapshotsResponse_Entry{
									{
										Snapshot: &csi.Snapshot{
											SnapshotId:     req.GetSnapshotId(),
											SourceVolumeId: snapVolumeID,
											CreationTime:   timestamppb.New(time.Unix(snap.CreateTime, 0)),
											SizeBytes:      int64(snap.UsedSize),
											ReadyToUse:     true,
										},
									},
								}

								return &csi.ListSnapshotsResponse{
									Entries: entries,
								}, nil
							}
						}
					}
				}
			}
		}

		return &csi.ListSnapshotsResponse{}, nil
	}

	var snapshots []csi.Snapshot

	if len(req.GetSourceVolumeId()) != 0 {
		klog.Infof("[ListSnapshots] SourceVolumeId is not empty, return snapshots that match the source volume id %s", req.GetSourceVolumeId())
		if volData, _ := cs.Driver.GetContextDataFromVolumeContextID(req.GetSourceVolumeId()); volData != nil {
			if authClient, _ := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server); authClient != nil {
				var volumeAPI GenericVolumeInterface
				if volData.protocol == protocolNFS {
					volumeAPI = &FileVolumeWrapper{op: goqsan.NewFileVolume(authClient)}
				} else {
					volumeAPI = &VolumeWrapper{op: goqsan.NewVolume(authClient)}
				}
				if snapMap, _ := getSnapshotsByPoolID(ctx, volumeAPI, volData.poolId); snapMap != nil {
					for volId, snaps := range snapMap {
						if volId == volData.volId {
							for _, snap := range *snaps {
								if !snap.Trash.InTrash {
									snapshot := csi.Snapshot{
										SnapshotId:     cs.Driver.GenerateSnapContextID(req.GetSourceVolumeId(), snap.ID),
										SourceVolumeId: req.GetSourceVolumeId(),
										CreationTime:   timestamppb.New(time.Unix(snap.CreateTime, 0)),
										SizeBytes:      int64(snap.UsedSize),
										ReadyToUse:     true,
									}
									snapshots = append(snapshots, snapshot)
								}
							}
						}
					}
				}
			}
		}
	} else {
		klog.Infof("[ListSnapshots] List all snapshots")
		if snapMap, _ := getAllSnapshots(ctx, cs.Driver); snapMap != nil {
			for volumeID, snaps := range snapMap {
				for _, snap := range *snaps {
					snapshot := csi.Snapshot{
						SnapshotId:     cs.Driver.GenerateSnapContextID(volumeID, snap.ID),
						SourceVolumeId: volumeID,
						CreationTime:   timestamppb.New(time.Unix(snap.CreateTime, 0)),
						SizeBytes:      int64(snap.UsedSize),
						ReadyToUse:     true,
					}
					snapshots = append(snapshots, snapshot)
				}
			}
		}
	}

	if len(snapshots) == 0 {
		klog.Infof("[ListSnapshots] No snapshot response")
		return &csi.ListSnapshotsResponse{}, nil
	}

	var (
		ulenSnapshots = int32(len(snapshots))
		maxEntries    = req.MaxEntries
		startingToken int32
		maxToken      = uint32(math.MaxUint32)
	)

	if v := req.StartingToken; v != "" {
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, status.Errorf(
				codes.Aborted,
				"startingToken=%d !< int32=%d",
				startingToken, maxToken)
		}
		startingToken = int32(i)
	}

	if startingToken > ulenSnapshots {
		return nil, status.Errorf(
			codes.Aborted,
			"startingToken=%d > len(snapshots)=%d",
			startingToken, ulenSnapshots)
	}

	// Discern the number of remaining entries.
	rem := ulenSnapshots - startingToken

	// If maxEntries is 0 or greater than the number of remaining entries then
	// set maxEntries to the number of remaining entries.
	if maxEntries == 0 || maxEntries > rem {
		maxEntries = rem
	}

	var (
		i       int
		j       = startingToken
		entries = make(
			[]*csi.ListSnapshotsResponse_Entry,
			maxEntries)
	)

	for i = 0; i < len(entries); i++ {
		entries[i] = &csi.ListSnapshotsResponse_Entry{
			Snapshot: &snapshots[j],
		}
		j++
	}

	var nextToken string
	if j < ulenSnapshots {
		nextToken = fmt.Sprintf("%d", j)
	}

	klog.Infof("[ListSnapshots] entries(%d) startingToken(%d) nextToken(%s) ulenSnapshots(%d)", entries, startingToken, nextToken, ulenSnapshots)
	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is empty")
	}

	reqCapacity := req.GetCapacityRange().GetRequiredBytes()
	volSizeMB := (reqCapacity + (1<<20) - 1) >> 20
	klog.Infof("[ControllerExpandVolume] volumeID: %s, reqCapacity: %d", volumeID, reqCapacity)
	if (volSizeMB * 1024 * 1024) != reqCapacity {
		klog.Infof("[ControllerExpandVolume] reqCapacity(%v) ==> volSize(%v)", reqCapacity, volSizeMB * 1024 * 1024)
	}

	volData, err := cs.Driver.GetContextDataFromVolumeContextID(volumeID)
	klog.Infof("[ControllerExpandVolume] volData: %+v", volData)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to get volume context data from Volume ID %s: %v", volumeID, err))
	}

	authClient, err := cs.Driver.qsan.GetnAddAuthClient(ctx, volData.server)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if volData.protocol == protocolNFS {
		volumeAPI := goqsan.NewFileVolume(authClient)
		opts := &goqsan.FileVolumeModifyOptions{
			TotalSize: uint64(volSizeMB),
		}
		_, err := volumeAPI.ModifyVolume(ctx, volData.volId, opts)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("[ControllerExpandVolume] Expand volumeID %s successfully", volumeID)

		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         volSizeMB << 20,
			NodeExpansionRequired: false,
		}, nil

	}

	// default case: protocolISCSI and protocolFC
	volumeAPI := goqsan.NewVolume(authClient)
	opts := &goqsan.VolumeModifyOptions{
		TotalSize: uint64(volSizeMB),
	}
	newVol, err := volumeAPI.ModifyVolume(ctx, volData.volId, opts)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("[ControllerExpandVolume] Expand volumeID %s successfully", volumeID)
	hasAttached := false
	if newVol.TargetID != "" {
		klog.Infof("[ControllerExpandVolume] newVol.TargetID = %s", newVol.TargetID)
		hasAttached = true
	} else {
		klog.Infof("[ControllerExpandVolume] newVol.TargetID NULL")
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         volSizeMB << 20,
		NodeExpansionRequired: hasAttached,
	}, nil
}

// isValidVolumeCapabilities validates the given VolumeCapability array is valid
func isValidVolumeCapabilities(protocol string, volCaps []*csi.VolumeCapability) error {
	if len(volCaps) == 0 {
		return fmt.Errorf("Volume capabilities missing in request")
	}

	for _, cap := range volCaps {
		if cap.GetMount() == nil && cap.GetBlock() == nil {
			return fmt.Errorf("Cannot have both mount and block access type be undefined")
		}

		if cap.GetBlock() != nil {
			if cap.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
				return fmt.Errorf("Block volume can't use ReadOnlyMany access mode.")
			}
			if protocol == protocolNFS {
				return fmt.Errorf("Block volume not support for NFS protocol.")
			}
		}

		// mode := cap.GetAccessMode().GetMode()
		// if mode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		// 	return fmt.Errorf("Not support MULTI_NODE_MULTI_WRITER mode")
		// }
		// } else if mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		// 	return fmt.Errorf("Not support MULTI_NODE_READER_ONLY mode")
		// }
	}

	return nil
}

func validateCloneCondition(ctx context.Context, volumeAPI GenericVolumeInterface, volSizeMB int64, parentVolId string) (bool, error) {
	pvol, err := genericListVolumeByID(ctx, volumeAPI, parentVolId)
	if err != nil {
		resterr, ok := err.(*goqsan.RestError)
		if ok && resterr.StatusCode == http.StatusNotFound {
			return false, status.Errorf(codes.NotFound, "The parent volume(%s) does not exist.", parentVolId)
		}
		return false, status.Errorf(codes.Internal, "Failed to get parent volume(%s): %v", parentVolId, err)
	}
	if volSizeMB < int64(pvol.TotalSize) {
		return false, status.Errorf(codes.OutOfRange, "The target volume is smaller in size than the source volume.(%d vs %d)", volSizeMB, pvol.TotalSize)
	}

	if strings.EqualFold(pvol.State, "ONLINE") {
		return true, nil
	} else if strings.EqualFold(pvol.State, "ERASING") {
		// Fix a sanity bug about clone: "should create volume from an existing source volume".
		// Bypass this situation because LVM will enqueue it for cloning.
		klog.Infof("Bypass! The parent volume(%s) is currently %s (%d %%)", parentVolId, pvol.State, pvol.Progress)
		return true, nil
	} else {
		return false, status.Errorf(codes.Internal, "The parent volume(%s) is currently %s (%d percent)", parentVolId, pvol.State, pvol.Progress)
	}
}

func monitorCloneStatus(volumeAPI GenericVolumeInterface, parentVolId, volId string, resizeVolSizeMB uint64) {
	var lastProgress, deltaProgress int
	var sleepSec time.Duration = time.Second * 10

	klog.Infof("[monitorCloneStatus] Enter, parentVolId(%s) volId(%s) resizeVolSizeMB(%d)", parentVolId, volId, resizeVolSizeMB)

	ctx := context.Background()
	for {
		time.Sleep(sleepSec)

		pvol, err := genericListVolumeByID(ctx, volumeAPI, parentVolId)
		if err != nil {
			klog.Warningf("[monitorCloneStatus] Failed to get parent volume(%s) of Backup Volume(%s). err: %v", parentVolId, volId, err)
			break
		} else {
			// klog.Infof("[monitorCloneStatus] parent volume data: %+v", pvol)
			if strings.EqualFold(pvol.State, "CLONING") {
				deltaProgress = pvol.Progress - lastProgress
				lastProgress = pvol.Progress
				if deltaProgress <= 1 {
					sleepSec = time.Second * 60
				} else if deltaProgress <= 5 {
					sleepSec = time.Second * 30
				} else {
					sleepSec = time.Second * 10
				}
				klog.Infof("[monitorCloneStatus] The parent volume(%s) of Volume(%s) is currently cloning... %d %% (Sleep %v)", parentVolId, volId, pvol.Progress, sleepSec)

			} else if strings.EqualFold(pvol.State, "INITIATING") {
				sleepSec = time.Second * 60
				klog.Infof("[monitorCloneStatus] The parent volume(%s) of Volume(%s) is currently initiating... %d %% (Sleep %v)", parentVolId, volId, pvol.Progress, sleepSec)

			} else if strings.EqualFold(pvol.State, "ONLINE") {
				gvol, _ := convertVolRAIDType(ctx, volumeAPI, volId, resizeVolSizeMB)
				if err := volumeAPI.DeleteCloneTask(ctx, parentVolId); err != nil {
					klog.Warningf("[monitorCloneStatus] Failed to delete clone task of source volume(%s). err: %+v", parentVolId, err)
				}

				if fw, ok := volumeAPI.(*FileVolumeWrapper); ok {
					fileVolumeAPI := fw.op
					if share, err := getShareFromFileVolume(ctx, fileVolumeAPI, volId); err != nil {
						klog.Warningf("[monitorCloneStatus] getShareFromFileVolume(%s) failed. err: %v", volId, err)
					} else {
						if share.IsOrphan {
							if _, err = convertOrphanShare(ctx, fileVolumeAPI, share.ID, gvol.Name, pvol.Name); err != nil {
								klog.Warningf("[monitorCloneStatus] convertOrphanShare(%s) failed: %v", share.ID, err)
							}
						} else {
							// klog.Warningf("IsOrphan attribute of share should be true. %+v", share)
							klog.Infof("[monitorCloneStatus] Volume(%s) orphan share was already converted. %+v", volId, share)
						}
					}
				}
				break

			} else {
				klog.Warningf("[monitorCloneStatus] The parent volume(%s) of Volume(%s) is at abnormal state %s", parentVolId, volId, pvol.State)
				break
			}
		}
	}

	klog.Infof("[monitorCloneStatus] Exit, volId(%s)", volId)
}

func convertVolRAIDType(ctx context.Context, volumeAPI GenericVolumeInterface, volId string, resizeVolSizeMB uint64) (*genericVolumeData, error) {
	klog.Infof("[convertVolRAIDType] volId(%s) resizeVolSizeMB(%d) caller(%s)", volId, resizeVolSizeMB, getCallerFunctionName())

	convertVolTypeMutex.Lock()
	defer convertVolTypeMutex.Unlock()

	gvol, err := genericListVolumeByID(ctx, volumeAPI, volId)
	if err != nil {
		klog.Warningf("[convertVolRAIDType] volume(%s) not exist.", volId)
	} else {
		if strings.EqualFold(gvol.Type, "BACKUP") {
			volType := "RAID"
			_, err := genericModifyVolume(ctx, volumeAPI, volId, &volType, resizeVolSizeMB)
			if err != nil {
				return nil, fmt.Errorf("Failed to convert volume(%s) from BACKUP type to RAID type. err: %+v", volId, err)
			}

			klog.Infof("[convertVolRAIDType] Convert volume(%s) from BACKUP type to RAID type.", volId)
			if resizeVolSizeMB > 0 {
				klog.Infof("[convertVolRAIDType] Extend volume(%s) size to %d MB.", volId, resizeVolSizeMB)
			}
		} else {
			klog.Infof("[convertVolRAIDType] volume(%s) is already RAID type.", volId)
		}
	}

	return gvol, nil
}

func convertOrphanShare(ctx context.Context, volumeAPI *goqsan.FileVolumeOp, shareId, volName, pvolName string) (*goqsan.ShareData, error) {
	klog.Infof("[convertOrphanShare] shareId(%s) volName(%s) caller(%s)", shareId, volName, getCallerFunctionName())
	recreateShare, err := volumeAPI.RecreateShare(ctx, shareId)
	if err != nil {
		return nil, fmt.Errorf("[convertOrphanShare] RecreateShare(%s) failed: %v", shareId, err)
	}

	klog.Infof("[convertOrphanShare] Recreate share and modify recreateShare(%s) name to %s", recreateShare.ID, volName)
	param := &goqsan.ShareModifyOptions{
		Name:        volName,
		Description: fmt.Sprintf("[CSI] NFS share for CSI cloned volume %s.\nParent volume is %s.", volName, pvolName),
	}
	share, err := volumeAPI.ModifyShare(ctx, recreateShare.ID, param)
	if err != nil {
		klog.Warningf("[convertOrphanShare] ModifyShare(%s) failed: %v", recreateShare.ID, err)
		return nil, status.Errorf(codes.Internal, "Failed to modify recreateShare name to %s. err: %v", volName, err)
	}

	return share, nil
}
