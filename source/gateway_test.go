// Copyright 2020 Blake Covarrubias
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"context"
	"testing"
	"time"

	"github.com/blake/external-mdns/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	v1 "sigs.k8s.io/gateway-api/apis/v1"
)

// mockIndexer implements cache.Indexer for testing
type mockIndexer struct {
	objects map[string]interface{}
}

func (m *mockIndexer) Add(obj interface{}) error {
	return nil
}

func (m *mockIndexer) Update(obj interface{}) error {
	return nil
}

func (m *mockIndexer) Delete(obj interface{}) error {
	return nil
}

func (m *mockIndexer) List() []interface{} {
	var list []interface{}
	for _, obj := range m.objects {
		list = append(list, obj)
	}
	return list
}

func (m *mockIndexer) ListKeys() []string {
	var keys []string
	for k := range m.objects {
		keys = append(keys, k)
	}
	return keys
}

func (m *mockIndexer) Get(obj interface{}) (interface{}, bool, error) {
	return nil, false, nil
}

func (m *mockIndexer) GetByKey(key string) (interface{}, bool, error) {
	obj, exists := m.objects[key]
	return obj, exists, nil
}

func (m *mockIndexer) Replace([]interface{}, string) error {
	return nil
}

func (m *mockIndexer) Resync() error {
	return nil
}

func (m *mockIndexer) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return nil, nil
}

func (m *mockIndexer) IndexKeys(indexName, indexKey string) ([]string, error) {
	return nil, nil
}

func (m *mockIndexer) ListIndexFuncValues(indexName string) []string {
	return nil
}

func (m *mockIndexer) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	return nil, nil
}

func (m *mockIndexer) AddIndexers(indexers cache.Indexers) error {
	return nil
}

func (m *mockIndexer) GetIndexers() cache.Indexers {
	return nil
}

// mockInformer implements cache.SharedIndexInformer for testing
type mockInformer struct {
	indexer cache.Indexer
}

func (m *mockInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (m *mockInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (m *mockInformer) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (m *mockInformer) RemoveEventHandler(handler cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (m *mockInformer) GetStore() cache.Store {
	return m.indexer
}

func (m *mockInformer) GetController() cache.Controller {
	return nil
}

func (m *mockInformer) Run(stopCh <-chan struct{}) {
}

func (m *mockInformer) RunWithContext(ctx context.Context) {
}

func (m *mockInformer) HasSynced() bool {
	return true
}

func (m *mockInformer) LastSyncResourceVersion() string {
	return ""
}

func (m *mockInformer) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	return nil
}

func (m *mockInformer) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	return nil
}

func (m *mockInformer) SetTransform(handler cache.TransformFunc) error {
	return nil
}

func (m *mockInformer) IsStopped() bool {
	return false
}

func (m *mockInformer) AddIndexers(indexers cache.Indexers) error {
	return nil
}

func (m *mockInformer) GetIndexer() cache.Indexer {
	return m.indexer
}

func TestGatewaySource_BuildRecords_Gateway(t *testing.T) {
	// Create a mock gateway informer with a gateway that has IP addresses
	gatewayIndexer := &mockIndexer{
		objects: make(map[string]interface{}),
	}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}

	httpRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		namespace:         "",
	}

	// Create a Gateway with IP addresses and hostname listener
	gateway := &v1.Gateway{
		Spec: v1.GatewaySpec{
			Listeners: []v1.Listener{
				{
					Hostname: (*v1.Hostname)(stringPtr("test.local")),
				},
			},
		},
		Status: v1.GatewayStatus{
			Addresses: []v1.GatewayStatusAddress{
				{
					Type:  (*v1.AddressType)(stringPtr(string(v1.IPAddressType))),
					Value: "192.168.1.1",
				},
			},
		},
	}

	records, err := source.buildRecords(gateway, resource.Added)
	if err != nil {
		t.Fatalf("buildRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.SourceType != "ingress" {
		t.Errorf("expected source type 'ingress', got %s", record.SourceType)
	}
	if record.Action != resource.Added {
		t.Errorf("expected action 'ADD', got %s", record.Action)
	}
	if len(record.Names) != 1 || record.Names[0] != "test" {
		t.Errorf("expected name 'test', got %v", record.Names)
	}
	if len(record.IPs) != 1 || record.IPs[0] != "192.168.1.1" {
		t.Errorf("expected IP '192.168.1.1', got %v", record.IPs)
	}
}

func TestGatewaySource_BuildRecords_HTTPRoute(t *testing.T) {
	// Create a mock gateway informer with a gateway that has IP addresses
	gatewayIndexer := &mockIndexer{
		objects: map[string]interface{}{
			"default/my-gateway": &v1.Gateway{
				Spec: v1.GatewaySpec{},
				Status: v1.GatewayStatus{
					Addresses: []v1.GatewayStatusAddress{
						{
							Type:  (*v1.AddressType)(stringPtr(string(v1.IPAddressType))),
							Value: "192.168.1.2",
						},
					},
				},
			},
		},
	}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}

	httpRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		namespace:         "",
	}

	// Create an HTTPRoute referencing the gateway
	httpRoute := &v1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
		},
		Spec: v1.HTTPRouteSpec{
			CommonRouteSpec: v1.CommonRouteSpec{
				ParentRefs: []v1.ParentReference{
					{
						Name: "my-gateway",
					},
				},
			},
			Hostnames: []v1.Hostname{"app.local"},
		},
	}

	records, err := source.buildRecords(httpRoute, resource.Added)
	if err != nil {
		t.Fatalf("buildRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.SourceType != "ingress" {
		t.Errorf("expected source type 'ingress', got %s", record.SourceType)
	}
	if record.Action != resource.Added {
		t.Errorf("expected action 'ADD', got %s", record.Action)
	}
	if len(record.Names) != 1 || record.Names[0] != "app" {
		t.Errorf("expected name 'app', got %v", record.Names)
	}
	if len(record.IPs) != 1 || record.IPs[0] != "192.168.1.2" {
		t.Errorf("expected IP '192.168.1.2', got %v", record.IPs)
	}
}

func TestGatewaySource_BuildRecords_HTTPRouteNoGateway(t *testing.T) {
	// Empty gateway informer (gateway not found)
	gatewayIndexer := &mockIndexer{objects: make(map[string]interface{})}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}

	httpRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		namespace:         "",
	}

	httpRoute := &v1.HTTPRoute{
		Spec: v1.HTTPRouteSpec{
			CommonRouteSpec: v1.CommonRouteSpec{
				ParentRefs: []v1.ParentReference{
					{
						Name: "non-existent-gateway",
					},
				},
			},
			Hostnames: []v1.Hostname{"app.local"},
		},
	}

	records, err := source.buildRecords(httpRoute, resource.Added)
	if err != nil {
		t.Fatalf("buildRecords failed: %v", err)
	}

	// Should return no records because gateway not found
	if len(records) != 0 {
		t.Fatalf("expected 0 records when gateway not found, got %d", len(records))
	}
}

func TestGatewaySource_BuildRecords_NonLocalHostname(t *testing.T) {
	gatewayIndexer := &mockIndexer{objects: make(map[string]interface{})}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}
	httpRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		namespace:         "",
	}

	// Gateway with non-.local hostname
	gateway := &v1.Gateway{
		Spec: v1.GatewaySpec{
			Listeners: []v1.Listener{
				{
					Hostname: (*v1.Hostname)(stringPtr("example.com")),
				},
			},
		},
		Status: v1.GatewayStatus{
			Addresses: []v1.GatewayStatusAddress{
				{
					Type:  (*v1.AddressType)(stringPtr(string(v1.IPAddressType))),
					Value: "192.168.1.1",
				},
			},
		},
	}

	records, err := source.buildRecords(gateway, resource.Added)
	if err != nil {
		t.Fatalf("buildRecords failed: %v", err)
	}

	// Should skip non-.local hostnames
	if len(records) != 0 {
		t.Fatalf("expected 0 records for non-.local hostname, got %d", len(records))
	}
}

func TestGatewaySource_NamespaceFiltering(t *testing.T) {
	gatewayIndexer := &mockIndexer{objects: make(map[string]interface{})}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}
	httpRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	// Source limited to \"allowed-namespace\"
	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		namespace:         "allowed-namespace",
	}

	// Gateway in different namespace
	gateway := &v1.Gateway{
		Spec: v1.GatewaySpec{
			Listeners: []v1.Listener{
				{
					Hostname: (*v1.Hostname)(stringPtr("test.local")),
				},
			},
		},
		Status: v1.GatewayStatus{
			Addresses: []v1.GatewayStatusAddress{
				{
					Type:  (*v1.AddressType)(stringPtr(string(v1.IPAddressType))),
					Value: "192.168.1.1",
				},
			},
		},
	}

	records, err := source.buildRecords(gateway, resource.Added)
	if err != nil {
		t.Fatalf("buildRecords failed: %v", err)
	}

	// Should be filtered out
	if len(records) != 0 {
		t.Fatalf("expected 0 records when namespace doesn't match, got %d", len(records))
	}
}

func TestGatewaySource_onGatewayAddForRoutes(t *testing.T) {
	gatewayIndexer := &mockIndexer{objects: make(map[string]interface{})}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}

	notifyChan := make(chan resource.Resource, 10)

	// HTTPRoute that references a gateway that doesn't exist yet
	httpRoute := &v1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-route",
			Namespace: "default",
		},
		Spec: v1.HTTPRouteSpec{
			CommonRouteSpec: v1.CommonRouteSpec{
				ParentRefs: []v1.ParentReference{
					{
						Name: "my-gateway",
					},
				},
			},
			Hostnames: []v1.Hostname{"app.local"},
		},
	}

	httpRouteIndexer := &mockIndexer{
		objects: map[string]interface{}{
			"default/my-route": httpRoute,
		},
	}
	httpRouteInformer := &mockInformer{indexer: httpRouteIndexer}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		notifyChan:        notifyChan,
		namespace:         "",
	}

	// 1. Try to process HTTPRoute when Gateway doesn't exist
	records, _ := source.buildRecords(httpRoute, resource.Added)
	if len(records) != 0 {
		t.Fatalf("expected 0 records when gateway doesn't exist, got %d", len(records))
	}

	// 2. Add the Gateway
	gateway := &v1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-gateway",
			Namespace: "default",
		},
		Status: v1.GatewayStatus{
			Addresses: []v1.GatewayStatusAddress{
				{
					Type:  (*v1.AddressType)(stringPtr(string(v1.IPAddressType))),
					Value: "192.168.1.10",
				},
			},
		},
	}
	gatewayIndexer.objects["default/my-gateway"] = gateway

	// 3. Trigger onGatewayAddForRoutes
	source.onGatewayAddForRoutes(gateway)

	// 4. Check if we got the Added record (it sends Deleted then Added)
	// We expect 2 records because updateRoutesForGateway sends Deleted then Added
	select {
	case res := <-notifyChan:
		if res.Action != resource.Deleted {
			t.Errorf("expected Deleted action first, got %s", res.Action)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for Deleted record")
	}

	select {
	case res := <-notifyChan:
		if res.Action != resource.Added {
			t.Errorf("expected Added action, got %s", res.Action)
		}
		if res.Names[0] != "app" {
			t.Errorf("expected name 'app', got %s", res.Names[0])
		}
		if res.IPs[0] != "192.168.1.10" {
			t.Errorf("expected IP '192.168.1.10', got %s", res.IPs[0])
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for Added record")
	}
}

func TestGatewaySource_BuildRecords_GRPCRoute(t *testing.T) {
	// Create a mock gateway informer with a gateway that has IP addresses
	gatewayIndexer := &mockIndexer{
		objects: map[string]interface{}{
			"default/my-gateway": &v1.Gateway{
				Spec: v1.GatewaySpec{},
				Status: v1.GatewayStatus{
					Addresses: []v1.GatewayStatusAddress{
						{
							Type:  (*v1.AddressType)(stringPtr(string(v1.IPAddressType))),
							Value: "192.168.1.3",
						},
					},
				},
			},
		},
	}
	gatewayInformer := &mockInformer{indexer: gatewayIndexer}

	httpRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}
	grpcRouteInformer := &mockInformer{indexer: &mockIndexer{objects: make(map[string]interface{})}}

	source := &GatewaySource{
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
		namespace:         "",
	}

	// Create a GRPCRoute referencing the gateway
	grpcRoute := &v1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
		},
		Spec: v1.GRPCRouteSpec{
			CommonRouteSpec: v1.CommonRouteSpec{
				ParentRefs: []v1.ParentReference{
					{
						Name: "my-gateway",
					},
				},
			},
			Hostnames: []v1.Hostname{"grpc-app.local"},
		},
	}

	records, err := source.buildRecords(grpcRoute, resource.Added)
	if err != nil {
		t.Fatalf("buildRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.SourceType != "ingress" {
		t.Errorf("expected source type 'ingress', got %s", record.SourceType)
	}
	if record.Action != resource.Added {
		t.Errorf("expected action 'ADD', got %s", record.Action)
	}
	if len(record.Names) != 1 || record.Names[0] != "grpc-app" {
		t.Errorf("expected name 'grpc-app', got %v", record.Names)
	}
	if len(record.IPs) != 1 || record.IPs[0] != "192.168.1.3" {
		t.Errorf("expected IP '192.168.1.3', got %v", record.IPs)
	}
}

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}
