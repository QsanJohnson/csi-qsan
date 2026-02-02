package driver

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"gitlab.qsan.com/sharedlibs-go/goqsan"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
	"k8s.io/klog/v2"
)

type StorageArraySecret struct {
	Server   string `yaml:"server"`
	Port     int    `yaml:"port"`
	Https    bool   `yaml:"https"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type StorageArraySecrets struct {
	StorageArraySecrets []StorageArraySecret `yaml:"storageArrays"`
}

type QsanClient struct {
	secrets    *StorageArraySecrets
	authClient map[string]*goqsan.AuthClient
}

func NewQsanClient(secretFile string) *QsanClient {
	return &QsanClient{
		secrets:    loadSecretFile(secretFile),
		authClient: make(map[string]*goqsan.AuthClient),
	}
}

func (c *QsanClient) MonitorSecretFile(secretFile string) {
	dir := filepath.Dir(secretFile)
	klog.Infof("Monitoring directory %s ...\n", dir)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		klog.Errorf("Unable to create a watcher: %v", err)
		return
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					klog.Warningf("watcher.Events no OK")
					break
				}
				klog.Infof("Cache event: %+v\n", event)
				if event.Op&fsnotify.Create == fsnotify.Create &&
					(event.Name == dir+"/..data" || event.Name == secretFile) {
					// Update secret
					c.secrets = loadSecretFile(secretFile)
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					klog.Warningf("watcher.Errors no OK")
					break
				}
				klog.Errorf("watcher.Errors: %v", err)
			}
		}
	}()

	if err := watcher.Add(dir); err != nil {
		klog.Errorf("Unable to monitor directory %s, err: %v", dir, err)
	}

	<-done
}

func loadSecretFile(secretFile string) *StorageArraySecrets {
	secretConfig := StorageArraySecrets{}

	yamlFile, err := ioutil.ReadFile(secretFile)
	if err == nil {
		if err = yaml.Unmarshal(yamlFile, &secretConfig); err != nil {
			klog.Errorf("Failed to parse secret: %v", err)
		}
	} else {
		klog.Errorf("Unable to open secret file: %v", err)
	}

	// Create a new StorageArraySecrets instance
	secretConfigCopy := &StorageArraySecrets{
		StorageArraySecrets: make([]StorageArraySecret, len(secretConfig.StorageArraySecrets)),
	}

	// Copy the fields from secretConfig to secretConfigCopy
	for i, secret := range secretConfig.StorageArraySecrets {
		secretConfigCopy.StorageArraySecrets[i] = secret
		// Replace the password field with a secure placeholder
		secretConfigCopy.StorageArraySecrets[i].Password = "********"
	}

	// Print the modified secretConfigCopy using formatted string
	klog.Infof("Secret file: %s ==>\n%+v\n", secretFile, secretConfigCopy)
	return &secretConfig
}

func (c *QsanClient) GetSecretData(ip string) *StorageArraySecret {
	var servers []string
	for _, client := range c.secrets.StorageArraySecrets {
		if client.Server == ip {
			return &client
		}
		servers = append(servers, client.Server)
	}

	klog.Warningf("IP(%s) is not found in storageArrays: %v\n", ip, servers)
	return nil
}

func (c *QsanClient) GetnAddAuthClient(ctx context.Context, ip string) (*goqsan.AuthClient, error) {

	if authClient, ok := c.authClient[ip]; ok {
		return authClient, nil
	} else {
		clientData := c.GetSecretData(ip)
		if clientData != nil {
			opt := goqsan.ClientOptions{ReqTimeout: 180 * time.Second, Https: clientData.Https, Port: clientData.Port}
			client := goqsan.NewClient(ip, opt)
			klog.Infof("Add a Qsan client ip(%s) username(%s), password(%s), https(%v), port(%d)", ip, clientData.Username, clientData.Password, clientData.Https, clientData.Port)
			authClient, err := client.GetAuthClient(ctx, clientData.Username, clientData.Password, goqsan.GetCSIScopes(clientData.Password))
			if err != nil {
				return nil, fmt.Errorf("failed to get authclient %s", ip)
			}

			c.authClient[ip] = authClient
			return authClient, nil
		} else {
			return nil, fmt.Errorf("failed to get secret data of client %s", ip)
		}
	}
}

func (c *QsanClient) UpdateAuthClient(ctx context.Context, inbandIp, outbandIp string) (*goqsan.AuthClient, error) {

	if authClient, ok := c.authClient[inbandIp]; ok {
		clientData := c.GetSecretData(inbandIp)
		opt := goqsan.ClientOptions{ReqTimeout: 180 * time.Second, Https: clientData.Https, Port: clientData.Port}
		client := goqsan.NewClient(outbandIp, opt)
		newAuthClient, err := client.GetAuthClient(ctx, clientData.Username, clientData.Password, goqsan.GetCSIScopes(clientData.Password))
		if err != nil {
			klog.Warningf("Failed to get new authclient %s. Use original authclient %s", outbandIp, inbandIp)
			return authClient, nil
		} else {
			klog.Infof("Update Qsan client management IP from %s to %s", inbandIp, outbandIp)
			c.authClient[inbandIp] = newAuthClient
			return newAuthClient, nil
		}
	} else {
		return nil, fmt.Errorf("%s not found in AuthClient", inbandIp)
	}
}
