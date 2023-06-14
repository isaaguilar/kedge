package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var mgr scheme.Builder

func init() {
	mgr.Register(&rbacv1.ClusterRoleBinding{})
	mgr.Register(&rbacv1.ClusterRole{})
	mgr.Register(&appsv1.StatefulSet{})

	// appsv1.AddToScheme
	// mgr.RegisterAll(appsv1.AddToScheme)
}

// func init() {
// 	// Register the API group and version.
// 	s :=
// 	AddToScheme = s.AddToScheme

// 	// AddToScheme(schema.GroupVersion{
// 	// 		Group:   "externaldns.k8s.io",
// 	// 		Version: "v1alpha1",
// 	// })
// }

func main() {
	b, err := ioutil.ReadFile("vcpretty.json")
	if err != nil {
		log.Fatal(err)
	}
	createOrUpdateResource(b, "default")
}

func kubernetesConfig(kubeconfigPath string) *rest.Config {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		log.Fatal("Failed to get config for clientset")
	}
	return config
}

func createOrUpdateResource(b []byte, namespace string) {

	config := kubernetesConfig(os.Getenv("KUBECONFIG"))
	ctx := context.TODO()

	obj := unstructured.Unstructured{}
	err := json.Unmarshal(b, &obj)
	if err != nil {
		log.Println("ERROR: could not unmarshal resource:", err)
		return
	}

	if obj.IsList() {
		obj.EachListItem(func(item runtime.Object) error {
			b, err := json.Marshal(item)
			if err != nil {
				return err
			}
			createOrUpdateResource(b, namespace)
			return nil
		})
		return
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	var dynamicClient dynamic.ResourceInterface
	namespaceableResourceClient, isNamespaced, err := getDynamicClientOnKind(gvk.GroupVersion().String(), gvk.Kind, config)
	if err != nil {
		log.Println("ERROR: could not get a client to handle resource:", err)
		return
	}
	if isNamespaced {
		dynamicClient = namespaceableResourceClient.Namespace(namespace)
	} else {
		dynamicClient = namespaceableResourceClient
	}

	// oldObj, err := dynamicClient.Get(ctx, obj.GetName(), metav1.GetOptions{})
	// if err != nil {
	// 	if !kerrors.IsNotFound(err) {
	// 		log.Printf("ERROR: failed fetching %s '%s/%s': %s", gvk.Kind, namespace, obj.GetName(), err)
	// 		return
	// 	}
	// } else {
	// 	oldObj.DeepCopyInto(&obj)
	// }
	obj.SetNamespace(namespace)
	obj.SetResourceVersion("")
	obj.SetUID("")
	obj.SetOwnerReferences([]metav1.OwnerReference{}) // TODO fix to original tf

	_, err = dynamicClient.Create(ctx, &obj, metav1.CreateOptions{})
	if err != nil {
		if kerrors.IsAlreadyExists(err) {
			log.Printf("%s '%s/%s' already exists. Updating resource", gvk.Kind, namespace, obj.GetName())
			_, err := dynamicClient.Update(ctx, &obj, metav1.UpdateOptions{})
			if err != nil {
				log.Printf("ERROR: could not update %s '%s/%s': %s", gvk.Kind, namespace, obj.GetName(), err)
				return
			}
			log.Printf("%s '%s/%s' has been updated", gvk.Kind, namespace, obj.GetName())
		} else {
			log.Printf("ERROR: could not create %s '%s/%s': %s", gvk.Kind, namespace, obj.GetName(), err)
		}
	} else {
		log.Printf("%s '%s/%s' has been created", gvk.Kind, namespace, obj.GetName())
	}
}

// getDynamicClientOnUnstructured returns a dynamic client on an Unstructured type. This client can be further namespaced.
func getDynamicClientOnKind(apiversion string, kind string, config *rest.Config) (dynamic.NamespaceableResourceInterface, bool, error) {
	gvk := schema.FromAPIVersionAndKind(apiversion, kind)
	apiRes, err := getAPIResourceForGVK(gvk, config)
	if err != nil {
		log.Printf("[ERROR] unable to get apiresource from unstructured: %s , error %s", gvk.String(), err)
		return nil, false, errors.Wrapf(err, "unable to get apiresource from unstructured: %s", gvk.String())
	}
	gvr := schema.GroupVersionResource{
		Group:    apiRes.Group,
		Version:  apiRes.Version,
		Resource: apiRes.Name,
	}

	intf, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Printf("[ERROR] unable to get dynamic client %s", err)
		return nil, false, err
	}
	res := intf.Resource(gvr)
	return res, apiRes.Namespaced, nil
}

func getAPIResourceForGVK(gvk schema.GroupVersionKind, config *rest.Config) (metav1.APIResource, error) {
	res := metav1.APIResource{}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		log.Printf("[ERROR] unable to create discovery client %s", err)
		return res, err
	}
	resList, err := discoveryClient.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		log.Printf("[ERROR] unable to retrieve resource list for: %s , error: %s", gvk.GroupVersion().String(), err)
		return res, err
	}
	for _, resource := range resList.APIResources {
		// if a resource contains a "/" it's referencing a subresource. we don't support suberesource for now.
		if resource.Kind == gvk.Kind && !strings.Contains(resource.Name, "/") {
			res = resource
			res.Group = gvk.Group
			res.Version = gvk.Version
			break
		}
	}
	return res, nil
}
