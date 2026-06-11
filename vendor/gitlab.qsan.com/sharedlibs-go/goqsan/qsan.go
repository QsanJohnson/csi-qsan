// @2022 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultHttpPort  = 80
	defaultHttpsPort = 443

	ModelSAN  = "SAN"
	ModelQSM4 = "QSM4"
)

const (
	// V1 version
	QERR_LVMERR_RG_NO_SPACE_V1          = 10104
	QERR_LVMERR_INVALID_VD_ID_V1        = 10300
	QERR_LVMERR_VD_ALREADY_DELETE_V1    = 10303
	QERR_LVMERR_VD_ALREADY_ATTACHED_V1  = 10405
	QERR_LVMERR_VD_IS_ALREADY_MAPPED_V1 = 11301
	QERR_UIERR_LUN_MAX_LIMIT_V1         = 11901
	QERR_LVMERR_TRY_LATER_V1            = 12002
	QERR_LVMERR_INVALID_ARG_V1          = 12003
	QERR_LVMERR_U_NAME_EXISTS_V1        = 12501
	QERR_LVMERR_TOO_MANY_SNAPS_V1       = 13501
	QERR_LVMERR_INVALID_SNAP_V1         = 13502
	QERR_LVMERR_SNAP_NOT_INITED_V1      = 13508
	QERR_UIIERR_SNAP_ALREADY_EXIST_V1   = 13514

	// V2 version
	QERR_LVMERR_RG_NO_SPACE_V2          = 5005104
	QERR_LVMERR_INVALID_VD_ID_V2        = 5000404
	QERR_LVMERR_VD_ALREADY_DELETE_V2    = 5005303
	QERR_LVMERR_VD_ALREADY_ATTACHED_V2  = 5005405
	QERR_LVMERR_VD_IS_ALREADY_MAPPED_V2 = 5005131
	QERR_UIERR_LUN_MAX_LIMIT_V2         = 5005191
	QERR_LVMERR_TRY_LATER_V2            = 5005998
	QERR_LVMERR_INVALID_ARG_V2          = 5005999
	QERR_LVMERR_U_NAME_EXISTS_V2        = 5005151
	QERR_LVMERR_TOO_MANY_SNAPS_V2       = 5005501
	QERR_LVMERR_INVALID_SNAP_V2         = 5005502
	QERR_LVMERR_SNAP_NOT_INITED_V2      = 5005508
	QERR_UIIERR_SNAP_ALREADY_EXIST_V2   = 5005154

	// Common
	QERR_LVMERR_FILEVOLUME_INVALID_ID   = 5000404
	QERR_LVMERR_SHARE_CREATE_FAILED     = 3030001
	QERR_LVMERR_SHARE_NAME_EXISTS       = 3030007
	QERR_LVMERR_SHARE_IS_MOUNTED        = 5005005
	QERR_LVMERR_SHARE_IN_HOSTGROUP      = 4002002
)

// QSAN client without authentication
type Client struct {
	apiKey     string
	baseURL    string
	HTTPClient *http.Client
}

// ClientOptions are options for QSAN http client.
type ClientOptions struct {
	Https      bool
	Port       int
	ReqTimeout time.Duration
}

// QSAN client with authentication
type AuthClient struct {
	Client
	user, passwd, scopes string
	accessToken          string
	refreshToken         string
}

// For authentication
type AuthRes struct {
	AccessToken  string `json:"accessToken"`
	ExpireTime   int    `json:"expireTime"`
	RefreshToken string `json:"refreshToken"`
}

// Empty response data
type EmptyData []interface{}

type errorResponse struct {
	Error struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"error"`
}

type RestError struct {
	ReqMethod  string
	ReqUrl     string
	StatusCode int
	ErrResp    errorResponse
	Err        error
}

func (r *RestError) Error() string {
	return fmt.Sprintf("[%s %s] status %d: %v (%d)", r.ReqMethod, r.ReqUrl, r.StatusCode, r.ErrResp.Error.Message, r.ErrResp.Error.Code)
}

// NewClient returns QSAN client with given URL
func NewClient(ip string, opts ClientOptions) *Client {
	client := &Client{}
	if opts.Https {
		port := defaultHttpsPort
		if opts.Port != 0 {
			port = opts.Port
		}

		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &Client{
			HTTPClient: &http.Client{Transport: tr},
			baseURL:    fmt.Sprintf("https://%s:%d", ip, port),
		}
	} else {
		port := defaultHttpPort
		if opts.Port != 0 {
			port = opts.Port
		}

		client = &Client{
			HTTPClient: &http.Client{},
			baseURL:    fmt.Sprintf("http://%s:%d", ip, port),
		}
	}

	if opts.ReqTimeout != 0 {
		client.HTTPClient.Timeout = opts.ReqTimeout
	}

	return client
}

func GetCSIScopes(passwd string) string {
	key := make([]byte, 32) //  32 bytes for AES-256
	copy(key[:], "qsanscope1234")
	enc := AESECBEncrypt([]byte(passwd), key)
	enc64 := base64.StdEncoding.EncodeToString(enc)

	return fmt.Sprintf("%s|%s", "csi.readwrite", enc64)
}

// If body format is url.Values, then body data will be sent using x-www-form-urlencoded format.
// If body format is string, then body data will be sent using raw data with JSON format.
func (c *Client) NewRequest(ctx context.Context, method, urlPath string, body interface{}) (*http.Request, error) {
	var (
		req *http.Request
		err error
	)

	urlStr := c.baseURL + urlPath
	klog.V(2).Infof("[NewRequest] %s url: %s\n", method, urlStr)
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	if body != nil {
		klog.V(3).Infof("[NewRequest] body: %v\n", body)
		switch body := body.(type) {
		case url.Values:
			req, err = http.NewRequest(method, u.String(), strings.NewReader(body.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		case string:
			// raw data
			req, err = http.NewRequest(method, u.String(), strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
		default:
			return nil, fmt.Errorf("Unknow body format! Only url.Values and string formats are supported.\n")
		}
	} else {
		req, err = http.NewRequest(method, u.String(), nil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	if err != nil {
		return nil, err
	}

	return req, nil
}

func (c *AuthClient) SendRequest(ctx context.Context, req *http.Request, v interface{}) error {
	resterr := RestError{ReqMethod: req.Method, ReqUrl: req.Host + req.URL.Path}
	res, err := c.doSendRequest(ctx, req, v)
	if err != nil {
		resterr.Err = err
		return &resterr
	}

	resterr.StatusCode = res.StatusCode
	if res.StatusCode == 401 {
		res.Body.Close()

		if req.URL.Path != "/auth/refresh" {
			// When the existing access token expired, generate a new access token.
			klog.V(2).Infof("[AuthSendRequest] generate new access token. (%s %s%s)\n", req.Method, req.Host, req.URL.Path)
			authRes, err := c.genAccessToken(ctx, c.refreshToken)
			if err != nil {
				resterr.Err = fmt.Errorf("genAccessToken failed: %v\n", err)
				return &resterr
			}

			// Update new access token then send request again
			c.accessToken = authRes.AccessToken
			c.apiKey = authRes.AccessToken
			klog.V(2).Infof("[AuthSendRequest] SendRequest again (%s %s%s)\n", req.Method, req.Host, req.URL.Path)
			res, err = c.doSendRequest(ctx, req, v)
		} else {
			// When refresh token expired, renew a new access token and refresh token.
			klog.V(2).Infof("[AuthSendRequest] renew new access token and refresh token.\n")
			res, err := c.login(ctx, c.user, c.passwd, c.scopes)
			if err != nil {
				resterr.Err = fmt.Errorf("renew access token failed: %v\n", err)
				return &resterr
			}

			// Update new access token and refresh token
			c.accessToken = res.AccessToken
			c.apiKey = res.AccessToken
			c.refreshToken = res.RefreshToken

			authRes, ok := v.(*AuthRes)
			if ok {
				*authRes = *res
			} else {
				klog.Errorf("[AuthSendRequest] Should no be here. (%s %s%s)\n", req.Method, req.Host, req.URL.Path)
			}

			return nil
		}

	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		if err = json.NewDecoder(res.Body).Decode(v); err != nil {
			klog.Warningf("[AuthSendRequest] %s %s%s, err: %+v\n", req.Method, req.Host, req.URL.Path, err)
			resterr.Err = err
			return &resterr
		}
		return nil

	} else if res.StatusCode == http.StatusNoContent {
		return nil

	} else {
		errRes := errorResponse{}
		if err = json.NewDecoder(res.Body).Decode(&errRes); err == nil {
			klog.Warningf("[AuthSendRequest] %s %s%s, StatusCode(%d) errRes: %+v\n", req.Method, req.Host, req.URL.Path, res.StatusCode, errRes)
			resterr.ErrResp = errRes
			return &resterr
		} else {
			klog.Warningf("[AuthSendRequest] %s %s%s, StatusCode(%d) err: %+v\n", req.Method, req.Host, req.URL.Path, res.StatusCode, err)
			resterr.Err = fmt.Errorf("unknown error, status code: %d", res.StatusCode)
			return &resterr
		}
	}
}

func (c *Client) SendRequest(ctx context.Context, req *http.Request, v interface{}) error {
	res, err := c.doSendRequest(ctx, req, v)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errRes := errorResponse{}
		if err = json.NewDecoder(res.Body).Decode(&errRes); err == nil {
			return errors.New(errRes.Error.Message)
		}

		return fmt.Errorf("unknown error, status code: %d", res.StatusCode)
	}

	if err = json.NewDecoder(res.Body).Decode(v); err != nil {
		return err
	}

	return err

}

func (c *Client) doSendRequest(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	start := time.Now()

	if c.apiKey != "" {
		klog.V(5).Infof("[doSendRequest] apiKey: %s\n", c.apiKey)
		req.Header.Set("Authorization", c.apiKey)
	}

	req = req.WithContext(ctx)
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		// Handle context deadline exceeded
		if errors.Is(err, context.DeadlineExceeded) {
			elapsed := time.Since(start)

			if deadline, ok := ctx.Deadline(); ok {
				timeout := deadline.Sub(start)
				klog.Errorf(
					"[doSendRequest] context deadline exceeded "+
						"(elapsed=%s, timeout=%s, deadline=%s, now=%s, url=%s%s)",
					elapsed, timeout, deadline.Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano), req.Host, req.URL.Path,
				)
			} else {
				klog.Errorf(
					"[doSendRequest] context deadline exceeded "+
						"(elapsed=%s, no deadline set, url=%s%s)",
					elapsed, req.Host, req.URL.Path)
			}
		} else {
			klog.Errorf("[doSendRequest] request failed: %v (elapsed=%s, url=%s%s)", err, time.Since(start), req.Host, req.URL.Path)
		}
		return nil, err
	}

	klog.V(4).Infof("[doSendRequest] StatusCode=%d elapsed=%s (%s%s)", res.StatusCode, time.Since(start), req.Host, req.URL.Path)
	return res, nil
}

func (c *Client) login(ctx context.Context, user, passwd, scopes string) (*AuthRes, error) {
	params := url.Values{}
	params.Add("user", user)
	params.Add("password", passwd)
	params.Add("offlineAccess", "true")
	params.Add("scopes", scopes)

	req, err := c.NewRequest(ctx, http.MethodPost, "/auth/get", params)
	if err != nil {
		return nil, err
	}

	res := AuthRes{}
	if err := c.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Generate a new access token from refresh token
func (c *AuthClient) genAccessToken(ctx context.Context, t string) (*AuthRes, error) {
	params := url.Values{}
	params.Add("refreshToken", t)

	req, err := c.NewRequest(ctx, http.MethodPost, "/auth/refresh", params)
	if err != nil {
		return nil, err
	}

	res := AuthRes{}
	if err := c.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (c *Client) GetAuthClient(ctx context.Context, user, passwd, scopes string) (*AuthClient, error) {
	res, err := c.login(ctx, user, passwd, scopes)
	if err != nil {
		return nil, fmt.Errorf("login failed: %v\n", err)
	}

	klog.V(3).Infof("AccessToken: %s\n", res.AccessToken)

	return &AuthClient{
		Client: Client{
			apiKey:     res.AccessToken,
			baseURL:    c.baseURL,
			HTTPClient: c.HTTPClient,
		},
		user:         user,
		passwd:       passwd,
		scopes:       scopes,
		accessToken:  res.AccessToken,
		refreshToken: res.RefreshToken,
	}, nil
}
