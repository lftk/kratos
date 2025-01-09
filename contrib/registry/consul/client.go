package consul

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"

	"github.com/hashicorp/consul/api"
)

// Datacenter represents the datacenter configuration.
// Deprecated: use string instead. An empty string represents the local datacenter, and "*" represents all datacenters.
type Datacenter = string

const (
	// SingleDatacenter represents the local datacenter.
	SingleDatacenter Datacenter = ""
	// MultiDatacenter represents all datacenters.
	MultiDatacenter Datacenter = "*"
)

// Client is consul client config
type Client struct {
	cli   *api.Client
	dcs   []string
	peers []string

	// used to normalize the dcs and peers
	normalized struct {
		once  sync.Once
		dcs   []string
		peers []string
	}

	// resolve service entry endpoints
	resolver ServiceResolver
	// healthcheck time interval in seconds
	healthcheckInterval int
	// heartbeat enable heartbeat
	heartbeat bool
	// deregisterCriticalServiceAfter time interval in seconds
	deregisterCriticalServiceAfter int
	// serviceChecks  user custom checks
	serviceChecks api.AgentServiceChecks

	// used to control heartbeat
	lock      sync.RWMutex
	cancelers map[string]*canceler

	// used to watch cluster changes
	clusterWatcher struct {
		ref    atomic.Int32
		value  atomic.Value
		cancel context.CancelFunc
		done   chan struct{}
		events sync.Map // map[context.Context]chan struct{}
	}
}

type canceler struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func defaultResolver(_ context.Context, entries []*api.ServiceEntry) []*registry.ServiceInstance {
	services := make([]*registry.ServiceInstance, 0, len(entries))
	for _, entry := range entries {
		var version string
		for _, tag := range entry.Service.Tags {
			ss := strings.SplitN(tag, "=", 2)
			if len(ss) == 2 && ss[0] == "version" {
				version = ss[1]
			}
		}
		endpoints := make([]string, 0)
		for scheme, addr := range entry.Service.TaggedAddresses {
			if scheme == "lan_ipv4" || scheme == "wan_ipv4" || scheme == "lan_ipv6" || scheme == "wan_ipv6" {
				continue
			}
			endpoints = append(endpoints, addr.Address)
		}
		if len(endpoints) == 0 && entry.Service.Address != "" && entry.Service.Port != 0 {
			endpoints = append(endpoints, fmt.Sprintf("http://%s:%d", entry.Service.Address, entry.Service.Port))
		}
		meta := entry.Service.Meta
		set := func(key, val string) {
			if val != "" {
				if meta == nil {
					meta = make(map[string]string)
				}
				meta[key] = val
			}
		}
		set("consul_datacenter", entry.Service.Datacenter)
		set("consul_peer", entry.Service.PeerName)
		services = append(services, &registry.ServiceInstance{
			ID:        entry.Service.ID,
			Name:      entry.Service.Service,
			Metadata:  meta,
			Version:   version,
			Endpoints: endpoints,
		})
	}
	return services
}

// ServiceResolver is used to resolve service endpoints
type ServiceResolver func(ctx context.Context, entries []*api.ServiceEntry) []*registry.ServiceInstance

// WatchService watches for changes in the specified service across Consul clusters.
// It returns a sequence of service instances that can be iterated over.
//
// The function follows these steps:
//  1. Watches for changes in Consul clusters and notifies when clusters are updated.
//  2. For each cluster, watches for changes in service entries, updates the entries map, and notifies when entries are updated.
//  3. Waits for notifications on the entries map, updates the service entries, and invokes the provided yield function with the updated entries.
func (c *Client) WatchService(ctx context.Context, service string, passingOnly bool) iterSeq[[]*registry.ServiceInstance] { //nolint:revive
	return func(yield func([]*registry.ServiceInstance) bool) {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var signalClusters signalValue[[]consulCluster]
		go func() {
			// Watch for changes in clusters.
			seq := c.watchClusters(cancelCtx)
			seq(func(clusters []consulCluster) bool {
				// Store the clusters, and signal the waiter.
				return signalClusters.Store(cancelCtx, clusters)
			})
		}()

		var signalEntries signalMap[string, []*api.ServiceEntry]
		go func() {
			cancelers := make(map[string]*canceler)
			for {
				// Wait for a notification on the clusters.
				ok := signalClusters.Wait(cancelCtx)
				if !ok {
					return
				}
				// Get the current list of clusters.
				clusters := signalClusters.Load()

				var deletions []string
				// Cancel all the old clusters that are no longer in the list.
				for old, cc := range cancelers {
					f := func(cluster consulCluster) bool { return cluster.String() == old }
					if containsFunc(clusters, f) {
						// This cluster is still in the list.
						continue
					}
					delete(cancelers, old)
					// Cancel the old cluster.
					cc.cancel()
					// Wait for the canceler to finish.
					<-cc.done
					// Mark the old cluster for deletion.
					deletions = append(deletions, old)
				}

				// Batch delete the old clusters.
				signalEntries.DeleteFunc(cancelCtx, func(cluster string, _ []*api.ServiceEntry) bool {
					return contains(deletions, cluster)
				})

				// Start watching the new clusters.
				for _, cluster := range clusters {
					cluster := cluster // capture the loop variable
					if _, ok := cancelers[cluster.String()]; ok {
						// This cluster is already being watched.
						continue
					}
					// Create a new canceler.
					ctx, cancel := context.WithCancel(cancelCtx)
					cc := &canceler{
						ctx:    ctx,
						cancel: cancel,
						done:   make(chan struct{}),
					}
					cancelers[cluster.String()] = cc
					go func() {
						defer close(cc.done)
						// Watch the cluster.
						seq := c.watchService(cc.ctx, cluster, service, passingOnly)
						seq(func(entries []*api.ServiceEntry) bool {
							// Store the entries, and signal the waiter.
							return signalEntries.Store(cc.ctx, cluster.String(), entries)
						})
					}()
				}
			}
		}()

		first := true
		for {
			// Wait for a notification.
			ok := signalEntries.Wait(cancelCtx)
			if !ok {
				return
			}
			// Get all entries.
			var entries []*api.ServiceEntry
			seq := signalEntries.All()
			seq(func(_ string, val []*api.ServiceEntry) bool {
				entries = append(entries, val...)
				return true
			})

			if first {
				// If it's the first time, wait for at least one entry.
				if len(entries) == 0 {
					continue
				}
				first = false
			}

			// Yield the entries.
			if !yield(c.resolver(cancelCtx, entries)) {
				return
			}
		}
	}
}

func (c *Client) clusterConfig() ([]string, []string) {
	c.normalized.once.Do(func() {
		// Normalize the list of datacenters.
		if contains(c.dcs, "*") {
			// If the datacenters list contains "*", it means all datacenters are allowed.
			c.normalized.dcs = []string{"*"}
		} else {
			// Remove duplicate datacenters from the list.
			dcs := uniq(c.dcs)
			if len(dcs) != 0 && contains(dcs, "") {
				// If the list contains an empty string (representing the local datacenter),
				// and there are multiple datacenters specified, remove the local datacenter.
				if info, err := c.cli.Agent().Self(); err == nil {
					localDatacenter, _ := info["Config"]["Datacenter"].(string) // This is safe, nil map == empty map
					if localDatacenter != "" {
						// Remove the local datacenter from the list.
						dcs = deleteFunc(dcs, func(dc string) bool { return dc == localDatacenter })
					}
				}
			}
			c.normalized.dcs = dcs
		}

		// Normalize the list of peers.
		if contains(c.peers, "*") {
			// If the peers list contains "*", it means all peers are allowed.
			c.normalized.peers = []string{"*"}
		} else {
			// Remove duplicate peers and any empty strings from the list.
			// An empty string is an invalid peer identifier, so it should be removed.
			c.normalized.peers = deleteFunc(uniq(c.peers), func(peer string) bool { return peer == "" })
		}
	})
	return c.normalized.dcs, c.normalized.peers
}

func (c *Client) listDatacenters(_ context.Context, waitIndex uint64) ([]consulCluster, uint64, error) {
	dcs, _ := c.clusterConfig()
	if isLocalDatacenter(dcs) {
		return []consulCluster{localDatacenter}, 0, nil
	}

	lastIndex := uint64(0)
	if isWildcard(dcs) {
		// If the wildcard is specified, list all datacenters.
		all, err := c.cli.Catalog().Datacenters()
		if err != nil {
			return nil, 0, err
		}
		dcs = all
		lastIndex = waitIndex + 1
	}

	clusters := make([]consulCluster, 0, len(dcs))
	for _, dc := range dcs {
		clusters = append(clusters, consulDatacenter(dc))
	}
	return clusters, lastIndex, nil
}

func (c *Client) watchDatacenters(ctx context.Context) iterSeq[[]consulCluster] {
	watch := func(ctx context.Context, waitIndex uint64) ([]consulCluster, uint64, error) {
		if waitIndex != 0 {
			// If it's not the first time, sleep for a while to avoid excessive polling.
			if err := sleepCtx(ctx, time.Minute); err != nil {
				return nil, 0, err
			}
		}
		return c.listDatacenters(ctx, waitIndex)
	}
	return watchSeq(ctx, watch)
}

func (c *Client) listPeers(ctx context.Context, waitIndex uint64) ([]consulCluster, uint64, error) {
	dcs, peers := c.clusterConfig()
	if isLocalDatacenter(dcs) {
		// If only fetching the local datacenter, no need to proceed.
		return nil, 0, nil
	}

	if !isWildcard(peers) {
		clusters := make([]consulCluster, 0, len(peers))
		for _, peer := range peers {
			if peer == "" {
				// Skip empty peers.
				continue
			}
			clusters = append(clusters, consulPeer(peer))
		}
		return clusters, 0, nil
	}

	opts := &api.QueryOptions{
		WaitIndex: waitIndex,
	}
	all, meta, err := c.cli.Peerings().List(ctx, opts)
	if err != nil {
		return nil, 0, err
	}

	clusters := make([]consulCluster, 0, len(all))
	for _, peer := range all {
		// Only include peers that are in the list of datacenters we care about.
		if contains(dcs, peer.Remote.Datacenter) {
			clusters = append(clusters, consulPeer(peer.Name))
		}
	}
	return clusters, meta.LastIndex, nil
}

func (c *Client) watchPeers(ctx context.Context) iterSeq[[]consulCluster] {
	if _, peers := c.clusterConfig(); len(peers) == 0 {
		// If no peers are configured, we don't need to watch for peers.
		return func(_ func([]consulCluster) bool) {}
	}
	return watchSeq(ctx, c.listPeers)
}

func (c *Client) getService(ctx context.Context, cluster consulCluster, service string, passingOnly bool, waitIndex uint64) ([]*api.ServiceEntry, uint64, error) { //nolint:lll
	opts := &api.QueryOptions{
		WaitIndex: waitIndex,
	}
	opts = opts.WithContext(ctx)
	cluster.applyQueryOptions(opts)
	entries, meta, err := c.cli.Health().Service(service, "", passingOnly, opts)
	if err != nil {
		return nil, 0, err
	}

	if _, ok := cluster.(consulDatacenter); ok {
		// If we're watching a datacenter, we don't need to filter by datacenter.
		return entries, meta.LastIndex, nil
	}

	dcs, _ := c.clusterConfig()
	if isWildcard(dcs) {
		// If we're watching all datacenters, we don't need to filter by datacenter.
		return entries, meta.LastIndex, nil
	}

	del := func(entry *api.ServiceEntry) bool {
		dc := entry.Service.Datacenter
		if dc == "" {
			dc = entry.Node.Datacenter
		}
		return !contains(dcs, dc)
	}
	return deleteFunc(entries, del), meta.LastIndex, nil
}

func (c *Client) watchService(ctx context.Context, cluster consulCluster, service string, passingOnly bool) iterSeq[[]*api.ServiceEntry] {
	watch := func(ctx context.Context, waitIndex uint64) ([]*api.ServiceEntry, uint64, error) {
		return c.getService(ctx, cluster, service, passingOnly, waitIndex)
	}
	return watchSeq(ctx, watch)
}

func (c *Client) listClusters(ctx context.Context) ([]consulCluster, error) {
	dcs, _, err := c.listDatacenters(ctx, 0)
	if err != nil {
		return nil, err
	}
	peers, _, err := c.listPeers(ctx, 0)
	if err != nil {
		return nil, err
	}
	return append(dcs, peers...), nil
}

// watchClusters watches for changes in Consul clusters and yields the results to the provided function.
//
// It ensures that only one instance of watchClustersInner runs at a time, even with multiple concurrent
// callers, by using a reference counter (clusters.ref) and shared context (clusters.cancel). This prevents
// redundant Consul API calls and ensures efficient resource usage.
func (c *Client) watchClusters(ctx context.Context) iterSeq[[]consulCluster] {
	return func(yield func([]consulCluster) bool) {
		type nullClusters = nullValue[[]consulCluster]

		event := make(chan struct{}, 1)
		c.clusterWatcher.events.Store(ctx, event)

		if c.clusterWatcher.ref.Add(1) == 1 {
			if c.clusterWatcher.done != nil {
				c.clusterWatcher.cancel()
				// Wait for the previous goroutine to finish.
				<-c.clusterWatcher.done
			}

			cancelCtx, cancel := context.WithCancel(context.Background())
			c.clusterWatcher.cancel = cancel
			c.clusterWatcher.done = make(chan struct{})

			go func() {
				defer close(c.clusterWatcher.done)
				seq := c.watchClustersInner(cancelCtx)
				seq(func(clusters []consulCluster) bool {
					// Update the clusters value and notify all waiters.
					c.clusterWatcher.value.Store(nullClusters{clusters})
					c.clusterWatcher.events.Range(func(key, value any) bool {
						select {
						case <-cancelCtx.Done():
							return false
						case <-key.(context.Context).Done():
						case value.(chan struct{}) <- struct{}{}:
						default:
						}
						return true
					})
					return cancelCtx.Err() == nil
				})
			}()
		}

	LOOP:
		for {
			// Read the current clusters value.
			v, ok := c.clusterWatcher.value.Load().(nullClusters)
			if ok {
				if !yield(v.value) {
					break
				}
			}
			// Wait for changes.
			select {
			case <-ctx.Done():
				break LOOP
			case <-event:
			}
		}

		c.clusterWatcher.events.Delete(ctx)

		cancel := c.clusterWatcher.cancel
		if c.clusterWatcher.ref.Add(-1) == 0 {
			if cancel != nil {
				cancel()
			}
		}
	}
}

func (c *Client) watchClustersInner(ctx context.Context) iterSeq[[]consulCluster] {
	return func(yield func([]consulCluster) bool) {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var dcsValue signalValue[[]consulCluster]
		go func() {
			// Watch datacenters.
			seq := c.watchDatacenters(cancelCtx)
			seq(func(dcs []consulCluster) bool {
				return dcsValue.Store(cancelCtx, dcs)
			})
		}()

		var peersValue signalValue[[]consulCluster]
		go func() {
			// Watch peers.
			seq := c.watchPeers(cancelCtx)
			seq(func(peers []consulCluster) bool {
				return peersValue.Store(cancelCtx, peers)
			})
		}()

		for {
			// Wait for changes.
			select {
			case <-cancelCtx.Done():
				return
			case <-dcsValue.Signal():
			case <-peersValue.Signal():
			}

			// Yield the clusters.
			dcs := dcsValue.Load()
			peers := peersValue.Load()
			if !yield(append(dcs, peers...)) {
				return
			}
		}
	}
}

func isNoDCPathErr(err error) bool {
	var se api.StatusError
	return errors.As(err, &se) && se.Code == 500 && se.Body == "No path to datacenter"
}

// GetService get service from consul
func (c *Client) GetService(ctx context.Context, service string, passingOnly bool) ([]*registry.ServiceInstance, error) {
	clusters, err := c.listClusters(ctx)
	if err != nil {
		return nil, err
	}
	var services []*registry.ServiceInstance
	for _, cluster := range clusters {
		entries, _, err := c.getService(ctx, cluster, service, passingOnly, 0)
		if err != nil {
			if isNoDCPathErr(err) {
				// ignore `No path to datacenter` error
				continue
			}
			return nil, err
		}
		services = append(services, c.resolver(ctx, entries)...)
	}
	return services, nil
}

// ListServices list services from consul
func (c *Client) ListServices(ctx context.Context) (map[string][]*registry.ServiceInstance, error) {
	clusters, err := c.listClusters(ctx)
	if err != nil {
		return nil, err
	}
	allServices := make(map[string][]*registry.ServiceInstance)
	for _, cluster := range clusters {
		services, _, err := c.listServices(ctx, cluster, 0)
		if err != nil {
			return nil, err
		}
		for service := range services {
			entries, _, err := c.getService(ctx, cluster, service, true, 0)
			if err != nil {
				if isNoDCPathErr(err) {
					// ignore `No path to datacenter` error
					continue
				}
				return nil, err
			}
			allServices[service] = append(allServices[service], c.resolver(ctx, entries)...)
		}
	}
	return allServices, nil
}

func (c *Client) listServices(ctx context.Context, cluster consulCluster, waitIndex uint64) (map[string][]string, uint64, error) {
	opts := &api.QueryOptions{
		WaitIndex: waitIndex,
	}
	opts = opts.WithContext(ctx)
	cluster.applyQueryOptions(opts)
	services, meta, err := c.cli.Catalog().Services(opts)
	if err != nil {
		return nil, 0, err
	}
	return services, meta.LastIndex, nil
}

// Register register service instance to consul
func (c *Client) Register(ctx context.Context, svc *registry.ServiceInstance, enableHealthCheck bool) error {
	addresses := make(map[string]api.ServiceAddress, len(svc.Endpoints))
	checkAddresses := make([]string, 0, len(svc.Endpoints))
	for _, endpoint := range svc.Endpoints {
		raw, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		addr := raw.Hostname()
		port, _ := strconv.ParseUint(raw.Port(), 10, 16)

		checkAddresses = append(checkAddresses, net.JoinHostPort(addr, strconv.FormatUint(port, 10)))
		addresses[raw.Scheme] = api.ServiceAddress{Address: endpoint, Port: int(port)}
	}
	asr := &api.AgentServiceRegistration{
		ID:              svc.ID,
		Name:            svc.Name,
		Meta:            svc.Metadata,
		Tags:            []string{fmt.Sprintf("version=%s", svc.Version)},
		TaggedAddresses: addresses,
	}
	if len(checkAddresses) > 0 {
		host, portRaw, _ := net.SplitHostPort(checkAddresses[0])
		port, _ := strconv.ParseInt(portRaw, 10, 32)
		asr.Address = host
		asr.Port = int(port)
	}
	if enableHealthCheck {
		for _, address := range checkAddresses {
			asr.Checks = append(asr.Checks, &api.AgentServiceCheck{
				TCP:                            address,
				Interval:                       fmt.Sprintf("%ds", c.healthcheckInterval),
				DeregisterCriticalServiceAfter: fmt.Sprintf("%ds", c.deregisterCriticalServiceAfter),
				Timeout:                        "5s",
			})
		}
		// custom checks
		asr.Checks = append(asr.Checks, c.serviceChecks...)
	}
	if c.heartbeat {
		asr.Checks = append(asr.Checks, &api.AgentServiceCheck{
			CheckID:                        "service:" + svc.ID,
			TTL:                            fmt.Sprintf("%ds", c.healthcheckInterval*2),
			DeregisterCriticalServiceAfter: fmt.Sprintf("%ds", c.deregisterCriticalServiceAfter),
		})
	}

	c.lock.Lock()
	if cc, ok := c.cancelers[svc.ID]; ok {
		cc.cancel()
		<-cc.done
	}
	var cc *canceler
	if c.heartbeat {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cc = &canceler{
			ctx:    cancelCtx,
			cancel: cancel,
			done:   make(chan struct{}),
		}
		c.cancelers[svc.ID] = cc
		go func() {
			<-cc.done
			cc.cancel()
			c.lock.Lock()
			if c.cancelers[svc.ID] == cc {
				delete(c.cancelers, svc.ID)
			}
			c.lock.Unlock()
		}()
	}
	c.lock.Unlock()

	err := c.cli.Agent().ServiceRegisterOpts(asr, api.ServiceRegisterOpts{}.WithContext(ctx))
	if err != nil {
		if c.heartbeat {
			close(cc.done)
		}
		return err
	}

	if c.heartbeat {
		go func() {
			defer close(cc.done)
			ticker := time.NewTicker(time.Second * time.Duration(c.healthcheckInterval))
			defer ticker.Stop()
			for {
				err = c.cli.Agent().UpdateTTLOpts("service:"+svc.ID, "pass", "pass", new(api.QueryOptions).WithContext(cc.ctx))
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					_ = c.cli.Agent().ServiceDeregister(svc.ID)
					return
				}
				if err != nil {
					log.Errorf("[Consul] update ttl heartbeat to consul failed! err=%v", err)
					// when the previous report fails, try to re register the service
					if err := sleepCtx(cc.ctx, time.Duration(rand.Intn(5))*time.Second); err != nil {
						_ = c.cli.Agent().ServiceDeregister(svc.ID)
						return
					}
					if err := c.cli.Agent().ServiceRegisterOpts(asr, api.ServiceRegisterOpts{}.WithContext(cc.ctx)); err != nil {
						log.Errorf("[Consul] re registry service failed!, err=%v", err)
					} else {
						log.Warn("[Consul] re registry of service occurred success")
					}
				}
				select {
				case <-cc.ctx.Done():
					_ = c.cli.Agent().ServiceDeregister(svc.ID)
					return
				case <-ticker.C:
				}
			}
		}()
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Deregister service by service ID
func (c *Client) Deregister(ctx context.Context, serviceID string) error {
	c.lock.RLock()
	cc, ok := c.cancelers[serviceID]
	c.lock.RUnlock()
	if ok {
		cc.cancel()
		<-cc.done
	}

	err := c.cli.Agent().ServiceDeregisterOpts(serviceID, new(api.QueryOptions).WithContext(ctx))
	var se api.StatusError
	if errors.As(err, &se) && se.Code == 404 {
		// not found
		err = nil
	}
	return err
}

const (
	// retryInterval is the base retry value.
	retryInterval = 5 * time.Second

	// maximum back off time, this is to prevent exponential runaway.
	maxBackoffTime = 180 * time.Second
)

// watchSeq is a helper function that watches for changes in Consul and yields the results to the provided function.
func watchSeq[T any](ctx context.Context, watch func(context.Context, uint64) (T, uint64, error)) iterSeq[T] {
	return func(yield func(T) bool) {
		failures := 0
		waitIndex := uint64(0)
		for {
			if err := ctx.Err(); err != nil {
				return
			}
			value, lastIndex, err := watch(ctx, waitIndex)
			if err != nil {
				failures++
				retry := retryInterval * time.Duration(failures*failures)
				if retry > maxBackoffTime {
					retry = maxBackoffTime
				}
				if err := sleepCtx(ctx, retry); err != nil {
					return
				}
				continue
			}
			if !yield(value) {
				return
			}
			if lastIndex == 0 {
				// No more data, we're done
				break
			}
			// Clear the failures
			failures = 0
			// Update the wait index
			waitIndex = lastIndex
		}
	}
}

// Aliases for types in the [iter] package. See https://pkg.go.dev/iter for more information.
type (
	iterSeq[V any]     func(yield func(V) bool)
	iterSeq2[K, V any] func(yield func(K, V) bool)
)

type consulCluster interface {
	String() string
	applyQueryOptions(opts *api.QueryOptions)
}

var (
	_ consulCluster = (*consulDatacenter)(nil)
	_ consulCluster = (*consulPeer)(nil)
)

var localDatacenter = consulDatacenter("")

type consulDatacenter string

func (dc consulDatacenter) String() string                        { return "dc:" + string(dc) }
func (dc consulDatacenter) applyQueryOptions(o *api.QueryOptions) { o.Datacenter = string(dc) }

type consulPeer string

func (p consulPeer) String() string                        { return "peer:" + string(p) }
func (p consulPeer) applyQueryOptions(o *api.QueryOptions) { o.Peer = string(p) }

func isWildcard(s []string) bool {
	return contains(s, "*") // Equivalent to `len(s) == 1 && s[0] == "*"`
}

func isLocalDatacenter(s []string) bool {
	return len(s) == 0 || (len(s) == 1 && s[0] == "")
}
