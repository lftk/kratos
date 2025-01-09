package consul

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/go-kratos/kratos/v2/registry"
)

func testClient(apiClient *api.Client, opts ...Option) *Client {
	return New(apiClient, opts...).cli
}

func testJoin(elems []string, sep string) string {
	switch {
	case elems == nil:
		return "(nil)"
	case len(elems) == 0:
		return "(empty)"
	default:
		copied := make([]string, len(elems))
		for i, e := range elems {
			if e == "" {
				e = "()"
			}
			copied[i] = e
		}
		return strings.Join(copied, sep)
	}
}

func TestClient_SingleDatacenter(t *testing.T) {
	apiClient, err := api.NewClient(&api.Config{Address: "127.0.0.1:8500"})
	if err != nil {
		t.Fatalf("create consul client failed: %v", err)
	}

	addr := fmt.Sprintf("%s:9091", getIntranetIP())
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Errorf("listen tcp %s failed!", addr)
		t.Fail()
	}
	defer lis.Close()
	go tcpServer(lis)

	serviceName := "server-1"
	services := []*registry.ServiceInstance{
		{
			ID:        "1",
			Name:      serviceName,
			Version:   "v0.0.1",
			Metadata:  nil,
			Endpoints: []string{fmt.Sprintf("tcp://%s?isSecure=false", addr)},
		},
		{
			ID:        "2",
			Name:      serviceName,
			Version:   "v0.0.1",
			Metadata:  nil,
			Endpoints: []string{fmt.Sprintf("tcp://%s?isSecure=false", addr)},
		},
	}

	const healthCheckInterval = time.Second

	opts := []Option{
		WithHeartbeat(true),
		WithHealthCheck(true),
		WithHealthCheckInterval(int(healthCheckInterval.Seconds())),
	}
	cli := testClient(apiClient, opts...)

	// register services
	for _, service := range services {
		service := service
		err := cli.Register(context.Background(), service, true)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			err = cli.Deregister(context.Background(), service.ID)
			if err != nil {
				t.Error(err)
				return
			}
		}()
	}

	// wait for health check
	time.Sleep(healthCheckInterval)

	for _, dcs := range [][]string{
		nil, {}, {""}, {"dc1"}, {"*"}, {"", "dc1"}, {"", "dc1", "*"}, {"", "dc1", "dc2", "*"},
	} {
		t.Run(testJoin(dcs, "_"), func(t *testing.T) {
			opts := []Option{
				WithDatacenter(dcs...),
			}
			cli := testClient(apiClient, opts...)

			// get service
			got, err := cli.GetService(context.Background(), serviceName, true)
			if err != nil {
				t.Error(err)
				return
			}

			if !reflect.DeepEqual(got, services) {
				t.Errorf("GetService, got=%v, want=%v", got, services)
				return
			}

			// list services
			allServices, err := cli.ListServices(context.Background())
			if err != nil {
				t.Error(err)
				return
			}

			// remove consul service
			delete(allServices, "consul")

			expectedAllServices := map[string][]*registry.ServiceInstance{
				serviceName: services,
			}
			if !reflect.DeepEqual(allServices, expectedAllServices) {
				t.Errorf("ListServices, got=%v, want=%v", allServices, expectedAllServices)
				return
			}

			// watch service
			seq := cli.WatchService(context.Background(), serviceName, true)
			seq(func(got []*registry.ServiceInstance) bool {
				if !reflect.DeepEqual(got, services) {
					t.Errorf("WatchService, got=%v, want=%v", got, services)
				}
				return false
			})
		})
	}
}

func TestClient_MultiDatacenter(t *testing.T) {
	apiClient1, err := api.NewClient(&api.Config{Address: "127.0.0.1:8500"})
	if err != nil {
		t.Fatalf("create consul client for dc1 failed: %v", err)
	}

	apiClient2, err := api.NewClient(&api.Config{Address: "127.0.0.1:8501"})
	if err != nil {
		t.Fatalf("create consul client for dc2 failed: %v", err)
	}

	addr := fmt.Sprintf("%s:9091", getIntranetIP())
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Errorf("listen tcp %s failed!", addr)
		t.Fail()
	}
	defer lis.Close()
	go tcpServer(lis)

	serviceName := "server-1"
	services1 := []*registry.ServiceInstance{
		{
			ID:        "1",
			Name:      serviceName,
			Version:   "v0.0.1",
			Metadata:  nil,
			Endpoints: []string{fmt.Sprintf("tcp://%s?isSecure=false", addr)},
		},
	}

	services2 := []*registry.ServiceInstance{
		{
			ID:        "2",
			Name:      serviceName,
			Version:   "v0.0.1",
			Metadata:  nil,
			Endpoints: []string{fmt.Sprintf("tcp://%s?isSecure=false", addr)},
		},
	}

	const healthCheckInterval = time.Second

	// Register services in both datacenters
	opts1 := []Option{
		WithHeartbeat(true),
		WithHealthCheck(true),
		WithHealthCheckInterval(int(healthCheckInterval.Seconds())),
	}
	cli1 := testClient(apiClient1, opts1...)

	for _, service := range services1 {
		service := service
		err := cli1.Register(context.Background(), service, true)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			err = cli1.Deregister(context.Background(), service.ID)
			if err != nil {
				t.Error(err)
				return
			}
		}()
	}

	opts2 := []Option{
		WithHeartbeat(true),
		WithHealthCheck(true),
		WithHealthCheckInterval(int(healthCheckInterval.Seconds())),
	}
	cli2 := testClient(apiClient2, opts2...)

	for _, service := range services2 {
		service := service
		err := cli2.Register(context.Background(), service, true)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			err = cli2.Deregister(context.Background(), service.ID)
			if err != nil {
				t.Error(err)
				return
			}
		}()
	}

	// Wait for health check
	time.Sleep(healthCheckInterval)

	// Test multi-datacenter service discovery
	for _, dcs := range [][]string{
		{"*"}, {"dc1", "dc2"}, {"*", "dc1", "dc2", "dc3"},
	} {
		t.Run(testJoin(dcs, "_"), func(t *testing.T) {
			opts := []Option{
				WithDatacenter(dcs...),
			}
			cli := testClient(apiClient1, opts...)

			// Get services from both datacenters
			got, err := cli.GetService(context.Background(), serviceName, true)
			if err != nil {
				t.Error(err)
				return
			}

			expectedServices := append(services1, services2...)
			if !reflect.DeepEqual(got, expectedServices) {
				t.Errorf("GetService, got=%v, want=%v", got, expectedServices)
				return
			}

			// List services from both datacenters
			allServices, err := cli.ListServices(context.Background())
			if err != nil {
				t.Error(err)
				return
			}

			// Remove consul service
			delete(allServices, "consul")

			expectedAllServices := map[string][]*registry.ServiceInstance{
				serviceName: expectedServices,
			}
			if !reflect.DeepEqual(allServices, expectedAllServices) {
				t.Errorf("ListServices, got=%v, want=%v", allServices, expectedAllServices)
				return
			}

			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			// Watch services from both datacenters
			seq := cli.WatchService(timeoutCtx, serviceName, true)
			seq(func(got []*registry.ServiceInstance) bool {
				sort.Slice(got, func(i, j int) bool {
					return got[i].ID < got[j].ID
				})
				return !reflect.DeepEqual(got, expectedServices)
			})

			if timeoutCtx.Err() != nil {
				t.Errorf("WatchService, got=%v, want=%v", timeoutCtx.Err(), nil)
			}
		})
	}
}
