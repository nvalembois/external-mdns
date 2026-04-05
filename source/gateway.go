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
	"fmt"
	"log"
	"strings"

	"github.com/blake/external-mdns/resource"
	"github.com/jpillora/go-tld"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	v1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayinformers "sigs.k8s.io/gateway-api/pkg/client/informers/externalversions"
)

// GatewaySource handles adding, updating, or removing mDNS record advertisements for Gateway resources
type GatewaySource struct {
	namespace         string
	notifyChan        chan<- resource.Resource
	gatewayInformer   cache.SharedIndexInformer
	httpRouteInformer cache.SharedIndexInformer
	grpcRouteInformer cache.SharedIndexInformer
}

// Run starts shared informers and waits for the shared informer cache to
// synchronize.
func (g *GatewaySource) Run(stopCh chan struct{}) error {
	go g.gatewayInformer.Run(stopCh)
	go g.httpRouteInformer.Run(stopCh)
	go g.grpcRouteInformer.Run(stopCh)
	if !cache.WaitForCacheSync(stopCh, g.gatewayInformer.HasSynced, g.httpRouteInformer.HasSynced, g.grpcRouteInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
	}
	return nil
}

func (g *GatewaySource) onAdd(obj interface{}) {
	advertiseRecords, err := g.buildRecords(obj, resource.Added)

	if err != nil {
		fmt.Println("Error adding ingress")
		return
	}

	for _, record := range advertiseRecords {
		g.notifyChan <- record
	}
}

func (g *GatewaySource) onDelete(obj interface{}) {
	advertiseRecords, err := g.buildRecords(obj, resource.Deleted)

	if err != nil {
		fmt.Println("Error deleting ingress")
		return
	}

	for _, record := range advertiseRecords {
		g.notifyChan <- record
	}
}

func (g *GatewaySource) onUpdate(oldObj interface{}, newObj interface{}) {
	oldResources, err1 := g.buildRecords(oldObj, resource.Updated)
	if err1 != nil {
		fmt.Printf("Error gathering old ingress resources: %s", err1)
	}

	for _, record := range oldResources {
		record.Action = resource.Deleted
		g.notifyChan <- record
	}

	newResources, err2 := g.buildRecords(newObj, resource.Updated)
	if err2 != nil {
		fmt.Printf("Error gathering new ingress resources: %s", err2)
	}

	for _, record := range newResources {
		record.Action = resource.Added
		g.notifyChan <- record
	}
}

func (g *GatewaySource) onHTTPRouteAdd(obj interface{}) {
	g.processRoute(obj, resource.Added)
}

func (g *GatewaySource) onHTTPRouteDelete(obj interface{}) {
	g.processRoute(obj, resource.Deleted)
}

func (g *GatewaySource) onHTTPRouteUpdate(oldObj interface{}, newObj interface{}) {
	g.processRoute(oldObj, resource.Deleted)
	g.processRoute(newObj, resource.Added)
}

func (g *GatewaySource) onGRPCRouteAdd(obj interface{}) {
	g.processRoute(obj, resource.Added)
}

func (g *GatewaySource) onGRPCRouteDelete(obj interface{}) {
	g.processRoute(obj, resource.Deleted)
}

func (g *GatewaySource) onGRPCRouteUpdate(oldObj interface{}, newObj interface{}) {
	g.processRoute(oldObj, resource.Deleted)
	g.processRoute(newObj, resource.Added)
}

func (g *GatewaySource) onGatewayAddForRoutes(obj interface{}) {
	gateway, ok := obj.(*v1.Gateway)
	if !ok {
		return
	}
	g.updateRoutesForGateway(gateway.Namespace, gateway.Name)
}

func (g *GatewaySource) onGatewayUpdateForRoutes(oldObj interface{}, newObj interface{}) {
	gateway, ok := newObj.(*v1.Gateway)
	if !ok {
		return
	}
	g.updateRoutesForGateway(gateway.Namespace, gateway.Name)
}

func (g *GatewaySource) onGatewayDeleteForRoutes(obj interface{}) {
	gateway, ok := obj.(*v1.Gateway)
	if !ok {
		return
	}
	g.updateRoutesForGateway(gateway.Namespace, gateway.Name)
}

func (g *GatewaySource) processRoute(obj interface{}, action string) {
	advertiseRecords, err := g.buildRecords(obj, action)
	if err != nil {
		log.Printf("Error processing route: %v", err)
		return
	}

	for _, record := range advertiseRecords {
		g.notifyChan <- record
	}
}

func (g *GatewaySource) updateRoutesForGateway(gatewayNamespace, gatewayName string) {
	// This is a simplified implementation
	// In production, you'd want to index routes by referenced Gateway
	// For now, we'll process all routes (inefficient but simple)
	httpRoutes := g.httpRouteInformer.GetIndexer().List()
	for _, obj := range httpRoutes {
		httpRoute, ok := obj.(*v1.HTTPRoute)
		if !ok {
			continue
		}

		// Check if this HTTPRoute references the changed Gateway
		referencesGateway := false
		for _, parentRef := range httpRoute.Spec.ParentRefs {
			refNamespace := httpRoute.Namespace
			if parentRef.Namespace != nil {
				refNamespace = string(*parentRef.Namespace)
			}

			if refNamespace == gatewayNamespace && string(parentRef.Name) == gatewayName {
				referencesGateway = true
				break
			}
		}

		if referencesGateway {
			// Namespace filtering
			if g.namespace != "" && httpRoute.Namespace != g.namespace {
				continue
			}
			// Send Deleted then Added to handle update (main.go only handles Added/Deleted)
			g.processRoute(httpRoute, resource.Deleted)
			g.processRoute(httpRoute, resource.Added)
		}
	}

	grpcRoutes := g.grpcRouteInformer.GetIndexer().List()
	for _, obj := range grpcRoutes {
		grpcRoute, ok := obj.(*v1.GRPCRoute)
		if !ok {
			continue
		}

		// Check if this GRPCRoute references the changed Gateway
		referencesGateway := false
		for _, parentRef := range grpcRoute.Spec.ParentRefs {
			refNamespace := grpcRoute.Namespace
			if parentRef.Namespace != nil {
				refNamespace = string(*parentRef.Namespace)
			}

			if refNamespace == gatewayNamespace && string(parentRef.Name) == gatewayName {
				referencesGateway = true
				break
			}
		}

		if referencesGateway {
			// Namespace filtering
			if g.namespace != "" && grpcRoute.Namespace != g.namespace {
				continue
			}
			// Send Deleted then Added to handle update (main.go only handles Added/Deleted)
			g.processRoute(grpcRoute, resource.Deleted)
			g.processRoute(grpcRoute, resource.Added)
		}
	}
}

func (g *GatewaySource) getGatewayIPs(parentRefs []v1.ParentReference, routeNamespace string) ([]string, error) {
	var ipFields []string

	for _, parentRef := range parentRefs {
		// Get referenced Gateway
		gatewayNamespace := routeNamespace
		if parentRef.Namespace != nil {
			gatewayNamespace = string(*parentRef.Namespace)
		}

		key := gatewayNamespace + "/" + string(parentRef.Name)
		obj, exists, err := g.gatewayInformer.GetIndexer().GetByKey(key)
		if err != nil || !exists {
			// Gateway might not exist yet
			continue
		}

		gateway, ok := obj.(*v1.Gateway)
		if !ok {
			continue
		}

		// Extract IP addresses from Gateway status
		for _, address := range gateway.Status.Addresses {
			if address.Type != nil && *address.Type == v1.IPAddressType {
				ipFields = append(ipFields, address.Value)
			}
		}
	}

	return ipFields, nil
}

func (g *GatewaySource) buildRecords(obj any, action string) ([]resource.Resource, error) {
	var records []resource.Resource

	// Handle Gateway resources
	if gateway, ok := obj.(*v1.Gateway); ok {
		// Namespace filtering
		if g.namespace != "" && gateway.Namespace != g.namespace {
			return records, nil
		}

		var ipFields []string
		for _, address := range gateway.Status.Addresses {
			if *address.Type == v1.IPAddressType {
				ipFields = append(ipFields, address.Value)
			}
		}

		if len(ipFields) == 0 {
			return records, nil
		}

		// Advertise each hostname under this Gateway
		var hostname string
		for _, listener := range gateway.Spec.Listeners {
			// Skip rules with no hostname
			if listener.Hostname == nil {
				continue
			}
			fakeURL := fmt.Sprintf("http://%s", string(*listener.Hostname))
			// Skip rules that do not use the .local TLD
			if !strings.HasSuffix(fakeURL, ".local") {
				continue
			}

			parsedHost, err := tld.Parse(fakeURL)
			if err != nil {
				log.Printf("Unable to parse hostname %s. %s", string(*listener.Hostname), err.Error())
				continue
			}

			if parsedHost.Subdomain != "" {
				hostname = fmt.Sprintf("%s.%s", parsedHost.Subdomain, parsedHost.Domain)
			} else {
				hostname = parsedHost.Domain
			}
			advertiseObj := resource.Resource{
				SourceType: "ingress",
				Action:     action,
				Names:      []string{hostname},
				Namespace:  gateway.Namespace,
				IPs:        ipFields,
			}

			records = append(records, advertiseObj)
		}
		return records, nil
	}

	// Handle HTTPRoute resources
	if httpRoute, ok := obj.(*v1.HTTPRoute); ok {
		// Namespace filtering
		if g.namespace != "" && httpRoute.Namespace != g.namespace {
			return records, nil
		}

		// Get IPs from referenced Gateways
		ipFields, err := g.getGatewayIPs(httpRoute.Spec.ParentRefs, httpRoute.Namespace)
		if err != nil {
			return records, err
		}
		if len(ipFields) == 0 {
			return records, nil
		}

		// Process hostnames
		for _, hostname := range httpRoute.Spec.Hostnames {
			// Skip hostnames that don't end with .local
			if !strings.HasSuffix(string(hostname), ".local") {
				continue
			}

			fakeURL := fmt.Sprintf("http://%s", string(hostname))
			parsedHost, err := tld.Parse(fakeURL)
			if err != nil {
				log.Printf("Unable to parse hostname %s. %s", string(hostname), err.Error())
				continue
			}

			var name string
			if parsedHost.Subdomain != "" {
				name = fmt.Sprintf("%s.%s", parsedHost.Subdomain, parsedHost.Domain)
			} else {
				name = parsedHost.Domain
			}

			advertiseObj := resource.Resource{
				SourceType: "ingress",
				Action:     action,
				Names:      []string{name},
				Namespace:  httpRoute.Namespace,
				IPs:        ipFields,
			}

			records = append(records, advertiseObj)
		}
		return records, nil
	}

	// Handle GRPCRoute resources
	if grpcRoute, ok := obj.(*v1.GRPCRoute); ok {
		// Namespace filtering
		if g.namespace != "" && grpcRoute.Namespace != g.namespace {
			return records, nil
		}

		// Get IPs from referenced Gateways
		ipFields, err := g.getGatewayIPs(grpcRoute.Spec.ParentRefs, grpcRoute.Namespace)
		if err != nil {
			return records, err
		}
		if len(ipFields) == 0 {
			return records, nil
		}

		// Process hostnames
		for _, hostname := range grpcRoute.Spec.Hostnames {
			// Skip hostnames that don't end with .local
			if !strings.HasSuffix(string(hostname), ".local") {
				continue
			}

			fakeURL := fmt.Sprintf("http://%s", string(hostname))
			parsedHost, err := tld.Parse(fakeURL)
			if err != nil {
				log.Printf("Unable to parse hostname %s. %s", string(hostname), err.Error())
				continue
			}

			var name string
			if parsedHost.Subdomain != "" {
				name = fmt.Sprintf("%s.%s", parsedHost.Subdomain, parsedHost.Domain)
			} else {
				name = parsedHost.Domain
			}

			advertiseObj := resource.Resource{
				SourceType: "ingress",
				Action:     action,
				Names:      []string{name},
				Namespace:  grpcRoute.Namespace,
				IPs:        ipFields,
			}

			records = append(records, advertiseObj)
		}
		return records, nil
	}

	// Not a Gateway, HTTPRoute or GRPCRoute
	return records, nil
}

// NewIngressWatcher creates an IngressSource
func NewGatewayWatcher(factory gatewayinformers.SharedInformerFactory, namespace string, notifyChan chan<- resource.Resource) GatewaySource {
	gatewayInformer := factory.Gateway().V1().Gateways().Informer()
	httpRouteInformer := factory.Gateway().V1().HTTPRoutes().Informer()
	grpcRouteInformer := factory.Gateway().V1().GRPCRoutes().Informer()

	g := &GatewaySource{
		namespace:         namespace,
		notifyChan:        notifyChan,
		gatewayInformer:   gatewayInformer,
		httpRouteInformer: httpRouteInformer,
		grpcRouteInformer: grpcRouteInformer,
	}

	// Gateway event handlers for Gateway resources
	_, err := gatewayInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    g.onAdd,
		DeleteFunc: g.onDelete,
		UpdateFunc: g.onUpdate,
	})
	if err != nil {
		log.Printf("Add Gateway watcher error %s\n", err)
	}

	// HTTPRoute event handlers
	_, err = httpRouteInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    g.onHTTPRouteAdd,
		DeleteFunc: g.onHTTPRouteDelete,
		UpdateFunc: g.onHTTPRouteUpdate,
	})
	if err != nil {
		log.Printf("Add HTTPRoute watcher error %s\n", err)
	}

	// GRPCRoute event handlers
	_, err = grpcRouteInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    g.onGRPCRouteAdd,
		DeleteFunc: g.onGRPCRouteDelete,
		UpdateFunc: g.onGRPCRouteUpdate,
	})
	if err != nil {
		log.Printf("Add GRPCRoute watcher error %s\n", err)
	}

	// Additional Gateway event handlers to update referencing routes when Gateway IPs change
	_, err = gatewayInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    g.onGatewayAddForRoutes,
		UpdateFunc: g.onGatewayUpdateForRoutes,
		DeleteFunc: g.onGatewayDeleteForRoutes,
	})
	if err != nil {
		log.Printf("Add Gateway watcher for route updates error %s\n", err)
	}

	return *g
}
