package consul

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/go-kratos/kratos/v2/registry"
)

var (
	_ registry.Registrar = (*Registry)(nil)
	_ registry.Discovery = (*Registry)(nil)
)

// Option is consul registry option.
type Option func(*Registry)

// WithHealthCheck with registry health check option.
func WithHealthCheck(enable bool) Option {
	return func(o *Registry) {
		o.enableHealthCheck = enable
	}
}

// WithTimeout with get services timeout option.
func WithTimeout(timeout time.Duration) Option {
	return func(o *Registry) {
		o.timeout = timeout
	}
}

// WithDatacenter sets the datacenter(s) for the registry.
// An empty string represents the local datacenter, and "*" represents all datacenters.
// This option allows the registry to operate within specific datacenters or across all datacenters.
func WithDatacenter(dcs ...string) Option {
	return func(o *Registry) {
		o.cli.dcs = dcs
	}
}

// WithPeer sets the peer(s) for the registry.
// If certain datacenters are on peer nodes, peer information must be provided.
// An empty string indicates no peer nodes are used, and "*" represents all peer nodes.
// Specifying a specific peer, if known, can improve efficiency by reducing unnecessary queries.
func WithPeer(peers ...string) Option {
	return func(o *Registry) {
		o.cli.peers = peers
	}
}

// WithHeartbeat enable or disable heartbeat.
func WithHeartbeat(enable bool) Option {
	return func(o *Registry) {
		if o.cli != nil {
			o.cli.heartbeat = enable
		}
	}
}

// WithServiceResolver with endpoint function option.
func WithServiceResolver(fn ServiceResolver) Option {
	return func(o *Registry) {
		if o.cli != nil {
			o.cli.resolver = fn
		}
	}
}

// WithHealthCheckInterval with healthcheck interval in seconds.
func WithHealthCheckInterval(interval int) Option {
	return func(o *Registry) {
		if o.cli != nil {
			o.cli.healthcheckInterval = interval
		}
	}
}

// WithDeregisterCriticalServiceAfter with deregister-critical-service-after in seconds.
func WithDeregisterCriticalServiceAfter(interval int) Option {
	return func(o *Registry) {
		if o.cli != nil {
			o.cli.deregisterCriticalServiceAfter = interval
		}
	}
}

// WithServiceCheck with service checks.
func WithServiceCheck(checks ...*api.AgentServiceCheck) Option {
	return func(o *Registry) {
		if o.cli != nil {
			o.cli.serviceChecks = checks
		}
	}
}

// Config is consul registry config
type Config struct {
	*api.Config
}

// Registry is consul registry
type Registry struct {
	cli               *Client
	enableHealthCheck bool
	registry          map[string]*serviceSet
	lock              sync.RWMutex
	timeout           time.Duration
}

// New creates consul registry
func New(apiClient *api.Client, opts ...Option) *Registry {
	r := &Registry{
		registry:          make(map[string]*serviceSet),
		enableHealthCheck: true,
		timeout:           10 * time.Second,
		cli: &Client{
			dcs:                            nil,
			peers:                          nil,
			cli:                            apiClient,
			resolver:                       defaultResolver,
			healthcheckInterval:            10,
			heartbeat:                      true,
			deregisterCriticalServiceAfter: 600,
			cancelers:                      make(map[string]*canceler),
		},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Register register service
func (r *Registry) Register(ctx context.Context, svc *registry.ServiceInstance) error {
	return r.cli.Register(ctx, svc, r.enableHealthCheck)
}

// Deregister deregister service
func (r *Registry) Deregister(ctx context.Context, svc *registry.ServiceInstance) error {
	return r.cli.Deregister(ctx, svc.ID)
}

// GetService returns service by name
func (r *Registry) GetService(ctx context.Context, name string) ([]*registry.ServiceInstance, error) {
	return r.cli.GetService(ctx, name, true)
}

// ListServices returns the service list.
func (r *Registry) ListServices() (map[string][]*registry.ServiceInstance, error) {
	return r.cli.ListServices(context.Background())
}

// Watch resolve service by name
func (r *Registry) Watch(ctx context.Context, name string) (registry.Watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.lock.Lock()
	set, ok := r.registry[name]
	if !ok {
		cancelCtx, cancel := context.WithCancel(context.Background())
		set = &serviceSet{
			registry:    r,
			watcher:     make(map[*watcher]struct{}),
			services:    &atomic.Value{},
			serviceName: name,
			ctx:         cancelCtx,
			cancel:      cancel,
		}
		r.registry[name] = set
	}
	set.ref.Add(1)
	r.lock.Unlock()

	// init watcher
	w := &watcher{
		event: make(chan struct{}, 1),
	}
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.set = set
	set.lock.Lock()
	set.watcher[w] = struct{}{}
	set.lock.Unlock()

	ss, _ := set.services.Load().([]*registry.ServiceInstance)
	if len(ss) > 0 {
		// If the service has a value, it needs to be pushed to the watcher,
		// otherwise the initial data may be blocked forever during the watch.
		select {
		case w.event <- struct{}{}:
		default:
		}
	}

	if !ok {
		go func() {
			// Once the Go version is upgraded to 1.23, the code can be refactored as follows:
			//
			// for services := range r.cli.WatchService(set.ctx, name, true) {
			// 	if err := set.ctx.Err(); err != nil {
			// 		break
			// 	}
			// 	set.broadcast(services)
			// }
			seq := r.cli.WatchService(set.ctx, name, true)
			seq(func(services []*registry.ServiceInstance) bool {
				if err := set.ctx.Err(); err != nil {
					return false
				}
				set.broadcast(services)
				return true
			})
		}()
	}
	return w, nil
}

func (r *Registry) tryDelete(ss *serviceSet) bool {
	if ss.ref.Add(-1) != 0 {
		return false
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	ss.cancel()
	delete(r.registry, ss.serviceName)
	return true
}
