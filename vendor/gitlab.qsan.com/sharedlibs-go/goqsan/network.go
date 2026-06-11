package goqsan

import (
	"context"
	"fmt"
	"net/http"
)

// NetworkOp handles network related methods of the QSAN storage.
type NetworkOp struct {
	client *AuthClient
}

type IPv4Info struct {
	Protocol         string `json:"protocol"`
	IP               string `json:"ip"`
	Submask          string `json:"submask"`
	Gateway          string `json:"gateway"`
	IsDefaultGateway bool   `json:"isDefaultGateway"`
	MacAddr          string `json:"macAddr,omitempty"` // Only NICDataQSM4 has
}

type NICDataSAN struct {
	ID               string   `json:"id"`
	Interface        string   `json:"interface"`
	Online           bool     `json:"online"`
	IsManagementPort bool     `json:"isManagementPort"`
	IsDataPort       bool     `json:"isDataPort"`
	MacAddr          string   `json:"macAddr"`
	Mtu              int      `json:"mtu"`
	Speed            int      `json:"speed"`
	Ipv4             IPv4Info `json:"ipv4"`
}

type NICDataQSM4 struct {
	ID               string   `json:"id"`
	Interface        string   `json:"interface"`
	Online           bool     `json:"online"`
	Speed            int      `json:"speed"`
	Mtu              int      `json:"mtu"`
	WakeOnLanSupport bool     `json:"wakeOnLanSupport"`
	WakeOnLanEnable  bool     `json:"wakeOnLanEnable"`
	RdmaSupport      bool     `json:"rdmaSupport"`
	Ipv4             IPv4Info `json:"ipv4"`
}

// NICData is a generic structure
type NICData struct {
	ID               string   `json:"id"`
	Interface        string   `json:"interface"`
	Online           bool     `json:"online"`
	MacAddr          string   `json:"macAddr,omitempty"`
	IsManagementPort bool     `json:"isManagementPort,omitempty"`
	IsDataPort       bool     `json:"isDataPort,omitempty"`
	Speed            int      `json:"speed,omitempty"`
	Mtu              int      `json:"mtu,omitempty"`
	WakeOnLanSupport bool     `json:"wakeOnLanSupport,omitempty"`
	WakeOnLanEnable  bool     `json:"wakeOnLanEnable,omitempty"`
	RdmaSupport      bool     `json:"rdmaSupport,omitempty"`
	Ipv4             IPv4Info `json:"ipv4"`
}

type ClusterData struct {
	Version    string `json:"version"`
	ClusterIps []struct {
		Interface string `json:"interface"`
		IP        string `json:"ip"`
		Submask   string `json:"submask"`
		PoolID    string `json:"poolId"`
		PoolName  string `json:"poolName"`
		Status    string `json:"status"`
		Node      string `json:"node"`
	} `json:"clusterIps"`
}

// NewNetwork returns network operation
func NewNetwork(client *AuthClient) *NetworkOp {
	return &NetworkOp{client}
}

// ListNICs list all network interfaces on SAN model
func (v *NetworkOp) ListNICsForSAN(ctx context.Context) (*[]NICDataSAN, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/network/ethernet", nil)
	if err != nil {
		return nil, err
	}

	res := []NICDataSAN{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListNICs list all network interfaces on QSM4 model
func (v *NetworkOp) ListNICsForQSM4(ctx context.Context) (*[]NICDataQSM4, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/network/ethernet", nil)
	if err != nil {
		return nil, err
	}

	res := []NICDataQSM4{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListNICs list all network interfaces on QSM4 model
func (v *NetworkOp) ListNICs(ctx context.Context, model string) (*[]NICData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/network/ethernet", nil)
	if err != nil {
		return nil, err
	}

	if model == "" {
		sysOp := NewSystem(&v.client.Client)
		info, _ := sysOp.GetAbout(ctx)
		if info.ModelType == "SAN" {
			model = ModelSAN
		} else {
			model = ModelQSM4
		}

		fmt.Printf("ListNICs auto detect, model=%s\n", model)
	}

	var res []NICData
	if model == ModelSAN {
		resSAN := []NICDataSAN{}
		if err := v.client.SendRequest(ctx, req, &resSAN); err != nil {
			return nil, err
		}
		for _, item := range resSAN {
			res = append(res, item.ToNICData())
		}
	} else {
		resQSM4 := []NICDataQSM4{}
		if err := v.client.SendRequest(ctx, req, &resQSM4); err != nil {
			return nil, err
		}
		for _, item := range resQSM4 {
			res = append(res, item.ToNICData())
		}
	}

	return &res, nil
}

func (san NICDataSAN) ToNICData() NICData {
	return NICData{
		ID:               san.ID,
		Interface:        san.Interface,
		Online:           san.Online,
		MacAddr:          san.MacAddr,
		IsManagementPort: san.IsManagementPort,
		IsDataPort:       san.IsDataPort,
		Speed:            san.Speed,
		Mtu:              san.Mtu,
		WakeOnLanSupport: false,
		WakeOnLanEnable:  false,
		RdmaSupport:      false,
		Ipv4:             san.Ipv4,
	}
}

func (qsm4 NICDataQSM4) ToNICData() NICData {
	return NICData{
		ID:               qsm4.ID,
		Interface:        qsm4.Interface,
		Online:           qsm4.Online,
		MacAddr:          qsm4.Ipv4.MacAddr,
		IsManagementPort: true,
		IsDataPort:       true,
		Speed:            qsm4.Speed,
		Mtu:              qsm4.Mtu,
		WakeOnLanSupport: qsm4.WakeOnLanSupport,
		WakeOnLanEnable:  qsm4.WakeOnLanEnable,
		RdmaSupport:      qsm4.RdmaSupport,
		Ipv4:             qsm4.Ipv4,
	}
}

// ListClusters list all Cluster IPs
func (v *NetworkOp) ListClusters(ctx context.Context) (*ClusterData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/network/cluster", nil)
	if err != nil {
		return nil, err
	}

	res := ClusterData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
