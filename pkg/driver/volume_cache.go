package driver

import (
	"sync"

	"gitlab.qsan.com/sharedlibs-go/goqsan"
)

var blockVolumeOpCache = struct {
	sync.RWMutex
	ops map[*goqsan.AuthClient]*goqsan.VolumeOp
}{
	ops: make(map[*goqsan.AuthClient]*goqsan.VolumeOp),
}

// newCachedVolumeOp reuses a block volume API per authenticated session.
// goqsan.NewVolume probes array information (including ModelType) during
// initialization, so caching the resulting operator avoids repeating that
// REST request for every CSI operation using the same AuthClient.
func newCachedVolumeOp(authClient *goqsan.AuthClient) *goqsan.VolumeOp {
	if authClient == nil {
		return nil
	}

	blockVolumeOpCache.RLock()
	volumeOp := blockVolumeOpCache.ops[authClient]
	blockVolumeOpCache.RUnlock()
	if volumeOp != nil {
		return volumeOp
	}

	blockVolumeOpCache.Lock()
	defer blockVolumeOpCache.Unlock()

	if volumeOp = blockVolumeOpCache.ops[authClient]; volumeOp != nil {
		return volumeOp
	}

	volumeOp = goqsan.NewVolume(authClient)
	if volumeOp != nil {
		blockVolumeOpCache.ops[authClient] = volumeOp
	}
	return volumeOp
}

func clearCachedVolumeOp(authClient *goqsan.AuthClient) {
	if authClient == nil {
		return
	}

	blockVolumeOpCache.Lock()
	delete(blockVolumeOpCache.ops, authClient)
	blockVolumeOpCache.Unlock()
}
