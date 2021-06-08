package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type GpmdRegistryDiscovery struct {
	*MultiServerDiscovery
	registry   string        //registry 即注册中心地址
	timeout    time.Duration //服务列表过期时间
	lastUpdate time.Time     //代表从注册中心更新服务列表的时间，默认10s过期。即10秒后需要从注册中心更新新的列表
}

const defaultUpdateDuration = time.Second * 10

func NewGpmdRegistryDiscovery(registerAddr string, timeout time.Duration) *GpmdRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateDuration
	}
	d := &GpmdRegistryDiscovery{
		MultiServerDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:             registerAddr,
		timeout:              timeout,
	}
	return d
}

func (d *GpmdRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *GpmdRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-GPMD-SERVERS"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *GpmdRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServerDiscovery.Get(mode)
}

func (d *GpmdRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServerDiscovery.GetAll()
}
