package driver

import (
	"context"
	"fmt"
	"sync"

	"gitlab.qsan.com/sharedlibs-go/goqsan"
)

var qsanModelCache = struct {
	sync.Mutex
	models map[*goqsan.AuthClient]string
}{
	models: make(map[*goqsan.AuthClient]string),
}

// getCachedQsanModel returns the storage model for an authenticated session.
// goqsan.NetworkOp.ListNICs auto-detects the model with the About REST API when
// the model argument is empty, so caching the model here lets callers pass it
// explicitly and avoid repeatedly probing About for the same AuthClient.
func getCachedQsanModel(ctx context.Context, authClient *goqsan.AuthClient) (string, error) {
	if authClient == nil {
		return "", fmt.Errorf("auth client is nil")
	}

	qsanModelCache.Lock()
	defer qsanModelCache.Unlock()

	model := qsanModelCache.models[authClient]
	if model != "" {
		return model, nil
	}

	systemAPI := goqsan.NewSystem(&authClient.Client)
	info, err := systemAPI.GetAbout(ctx)
	if err != nil {
		return "", err
	}
	if info == nil {
		return "", fmt.Errorf("empty about response")
	}

	if info.ModelType == goqsan.ModelSAN {
		model = goqsan.ModelSAN
	} else {
		model = goqsan.ModelQSM4
	}

	qsanModelCache.models[authClient] = model
	return model, nil
}

func clearCachedQsanModel(authClient *goqsan.AuthClient) {
	if authClient == nil {
		return
	}

	qsanModelCache.Lock()
	delete(qsanModelCache.models, authClient)
	qsanModelCache.Unlock()
}
