package driver

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

type volumeContextData struct {
	protocol string
	server   string
	username string
	password string
	poolId   string
	volId    string
	shareId  string
	targetId string
}

type snapContextData struct {
	*volumeContextData
	snapId string
}

// DriverOptions defines driver parameters specified in driver deployment
type DriverOptions struct {
	NodeID            string
	NodeIP            string
	DriverName        string
	Endpoint          string
	WorkingMountDir   string
	MaxVolumesPerNode int64
	SecretFile        string
}

type VolumeStat struct {
	CapacityBytes       int64
	Abnormal            bool
	Message             string
	LastUpdate          time.Time
	LastUpdateTimeStamp time.Time
}

type Driver struct {
	name              string
	nodeID            string
	nodeIP            string
	version           string
	buildDate         string
	endpoint          string
	maxVolumesPerNode int64

	ns          *NodeServer
	cscap       []*csi.ControllerServiceCapability
	nscap       []*csi.NodeServiceCapability
	volumeLocks *VolumeLocks

	qsan             *QsanClient
	volStatMap       map[string]VolumeStat
	fsCreateLimiter  *ThresholdLimiter
	fsPublishLimiter *ThresholdLimiter
}

const (
	DefaultDriverName     = "xevo.csi.qsan.com"
	DefaultIscsiPort      = 3260
	DefaultIscsiTimeout   = 5000
	DefaultFsType         = "ext4"
	tsUpdateVolPeriodMin  = 60
	paramProtocol         = "protocol"
	paramServer           = "server"
	paramPool             = "pool"
	paramPoolId           = "poolid"
	paramBlockSize        = "blocksize"
	paramCapabilities     = "capabilities"
	paramIscsiPortals     = "iscsiportals"
	paramIscsiTargets     = "iscsitargets"
	paramTarget           = "target"
	paramParentVolId      = "parentvolid"
	paramResize           = "resizemb"
	paramBgIoPriority     = "bgiopriority"
	paramIoPriority       = "iopriority"
	paramTargetRespmTime  = "targetrespmtime"
	paramMaxIops          = "maxiops"
	paramMaxThroughputKB  = "maxthroughputkb"
	paramMultipathEnabled = "multipathenabled"
	paramMgmtIp           = "mgmtip"
	paramShareName        = "sharename"
	paramSecurity         = "security"

	protocolISCSI = "iscsi"
	protocolFC    = "fc"
	protocolNFS   = "nfs"
)

var supportProtocols = []string{protocolISCSI, protocolFC, protocolNFS}
var supportFsTypes = []string{"ext3", "ext4", "xfs"}
var supportNfsVers = []string{"3", "4", "4.0", "4.1", "4.2"}
var supportKrb5Sec = []string{"krb5", "krb5i", "krb5p"}

func NewDriver(options *DriverOptions) *Driver {
	klog.Infof("Driver: %v version: %v (build %v)", options.DriverName, driverVersion, buildDate)

	n := &Driver{
		name:              options.DriverName,
		version:           driverVersion,
		buildDate:         buildDate,
		nodeID:            options.NodeID,
		nodeIP:            options.NodeIP,
		endpoint:          options.Endpoint,
		maxVolumesPerNode: options.MaxVolumesPerNode,
		qsan:              NewQsanClient(options.SecretFile),
		volStatMap:        make(map[string]VolumeStat),
		fsCreateLimiter:   NewThresholdLimiter(4),
		fsPublishLimiter:  NewThresholdLimiter(8),
	}

	go n.qsan.MonitorSecretFile(options.SecretFile)

	n.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
	})

	n.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_UNKNOWN,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_VOLUME_CONDITION,
	})
	n.volumeLocks = NewVolumeLocks()
	return n
}

func NewNodeServer(n *Driver /*, mounter mount.Interface*/) *NodeServer {
	return &NodeServer{
		Driver: n,
		mounter: &mount.SafeFormatAndMount{
			Interface: mount.New(""),
			Exec:      exec.New(),
		},
	}
}

func (n *Driver) Run(testMode bool) {
	versionMeta, err := GetVersionYAML(n.name)
	if err != nil {
		klog.Fatalf("%v", err)
	}
	klog.V(2).Infof("\nDRIVER INFORMATION:\n-------------------\n%s\n\nStreaming logs below:", versionMeta)

	n.ns = NewNodeServer(n)
	s := NewNonBlockingGRPCServer()
	s.Start(n.endpoint,
		NewDefaultIdentityServer(n),
		NewControllerServer(n),
		n.ns,
		testMode)
	s.Wait()
}

func (n *Driver) AddControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) {
	var csc []*csi.ControllerServiceCapability
	for _, c := range cl {
		csc = append(csc, NewControllerServiceCapability(c))
	}
	n.cscap = csc
}

func (n *Driver) AddNodeServiceCapabilities(nl []csi.NodeServiceCapability_RPC_Type) {
	var nsc []*csi.NodeServiceCapability
	for _, n := range nl {
		nsc = append(nsc, NewNodeServiceCapability(n))
	}
	n.nscap = nsc
}

func (n *Driver) GenerateVolumeContextID(protocol, ipv4, poolId, volId string, shareId, targetId *string) string {
	if protocol == protocolNFS {
		return fmt.Sprintf("%s-%s-%s-%s-%s-%s", protocol, ipv4ToHexStr(ipv4), poolId, volId, *shareId, *targetId)
	} else {
		return fmt.Sprintf("%s-%s-%s-%s", protocol, ipv4ToHexStr(ipv4), poolId, volId)
	}
}

func (n *Driver) GetContextDataFromVolumeContextID(id string) (*volumeContextData, error) {
	var shareId, targetId string
	tokens := strings.Split(id, "-")
	if len(tokens) == 4 && (tokens[0] == protocolISCSI || tokens[0] == protocolFC) {
		// Pass
	} else if len(tokens) == 6 && tokens[0] == protocolNFS {
		// Pass
		shareId = tokens[4]
		targetId = tokens[5]
	} else {
		return nil, fmt.Errorf("Format error with volume context ID %s ", id)
	}

	a, _ := hex.DecodeString(tokens[1])
	ipv4 := fmt.Sprintf("%d.%d.%d.%d", a[0], a[1], a[2], a[3])

	var username, password string
	var arrayServers []string
	for _, client := range n.qsan.secrets.StorageArraySecrets {
		arrayServers = append(arrayServers, client.Server)
		if client.Server == ipv4 {
			username, password = client.Username, client.Password
			break
		}
	}
	if username == "" && password == "" {
		klog.Warningf("[GetContextDataFromVolumeContextID] Volume IP(%s) not found in ArraySecrets %v", ipv4, arrayServers)
		username = "admin"
		password = ""
	}

	return &volumeContextData{
		protocol: tokens[0],
		server:   ipv4,
		poolId:   tokens[2],
		volId:    tokens[3],
		shareId:  shareId,
		targetId: targetId,
		username: username,
		password: password,
	}, nil
}

func (n *Driver) GenerateSnapContextID(volumeID, snapId string) string {
	return fmt.Sprintf("%s-%s", volumeID, snapId)
}

func (n *Driver) GetContextDataFromSnapContextID(id string) (*snapContextData, error) {
	var tokens []string

	if strings.HasPrefix(id, protocolNFS+"-") {
		tokens = strings.SplitN(id, "-", 7)
		if len(tokens) != 7 {
			return nil, fmt.Errorf("Format error with file snapshot context ID %s ", id)
		}

		if volData, err := n.GetContextDataFromVolumeContextID(fmt.Sprintf("%s-%s-%s-%s-%s-%s", tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5])); err != nil {
			return nil, fmt.Errorf("Failed to get volume context data. err: %v", err)
		} else {
			return &snapContextData{
				volumeContextData: volData,
				snapId:            tokens[6],
			}, nil
		}
	}

	// default case: protocolISCSI and protocolFC
	tokens = strings.SplitN(id, "-", 5)
	if len(tokens) != 5 {
		return nil, fmt.Errorf("Format error with snapshot context ID %s ", id)
	}

	if volData, err := n.GetContextDataFromVolumeContextID(fmt.Sprintf("%s-%s-%s-%s", tokens[0], tokens[1], tokens[2], tokens[3])); err != nil {
		return nil, fmt.Errorf("Failed to get volume context data. err: %v", err)
	} else {
		return &snapContextData{
			volumeContextData: volData,
			snapId:            tokens[4],
		}, nil
	}
}

func (n *Driver) GetVolumeContextIDFromSnapContextID(id string) (string, error) {
	var tokens []string

	if strings.HasPrefix(id, protocolNFS+"-") {
		tokens = strings.SplitN(id, "-", 7)
		if len(tokens) != 7 {
			return "", fmt.Errorf("Format error with snapshot context ID %s ", id)
		}

		return fmt.Sprintf("%s-%s-%s-%s-%s-%s", tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5]), nil
	}

	// default case: protocolISCSI and protocolFC
	tokens = strings.SplitN(id, "-", 5)
	if len(tokens) != 5 {
		return "", fmt.Errorf("Format error with snapshot context ID %s ", id)
	}

	return fmt.Sprintf("%s-%s-%s-%s", tokens[0], tokens[1], tokens[2], tokens[3]), nil
}

func parseVolumeContext(context map[string]string) (string, string, string, string, string, string, string, bool, uint64) {
	var protocol, iscsiPortals, iscsiTargets, shareName, targetName, parentVolId, security string
	var multipathEnabled bool
	var resizeVolSizeMB uint64
	for k, v := range context {
		switch strings.ToLower(k) {
		case paramProtocol:
			protocol = v
		case paramIscsiPortals:
			iscsiPortals = v
		case paramIscsiTargets:
			iscsiTargets = v
		case paramShareName:
			shareName = v
		case paramTarget:
			targetName = v
		case paramParentVolId:
			parentVolId = v
		case paramSecurity:
			security = v
		case paramMultipathEnabled:
			multipathEnabled, _ = strconv.ParseBool(v)
		case paramResize:
			resizeVolSizeMB, _ = strconv.ParseUint(v, 10, 64)
		}
	}

	return protocol, iscsiPortals, iscsiTargets, shareName, targetName, parentVolId, security, multipathEnabled, resizeVolSizeMB
}
