package goqsan

import (
	"context"
	"net/http"
)

// PoolOp handles pool related methods of the QSAN storage.
type PoolOp struct {
	client *AuthClient
}

type PoolData struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Provision    string `json:"provision"`
	AutoTiering  bool   `json:"autoTiering"`
	DedupEnabled bool   `json:"isDedup"`
	RaidLevel    string `json:"raidLevel"`
	NumOfVolumes int    `json:"numOfVolumes"`
}

// NewVolume returns volume operation
func NewPool(client *AuthClient) *PoolOp {
	return &PoolOp{client}
}

// ListPools list all pools
func (v *PoolOp) ListPools(ctx context.Context) (*[]PoolData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/pools", nil)
	if err != nil {
		return nil, err
	}

	res := []PoolData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListPoolByID list a dedicated pool with poolId
func (v *PoolOp) ListPoolByID(ctx context.Context, poolId string) (*PoolData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/pools/"+poolId, nil)
	if err != nil {
		return nil, err
	}

	res := PoolData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Search pools under given pool name
// func (v *PoolOp) ListPoolsBySearchPoolName(ctx context.Context, searchPoolName string) (*[]PoolData, error) {

// 	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/storage/pools?q=name='"+searchPoolName+"'", nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res := []PoolData{}
// 	if err := v.client.SendRequest(ctx, req, &res); err != nil {
// 		return nil, err
// 	}
// 	return &res, nil
// }
