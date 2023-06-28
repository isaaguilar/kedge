package kedge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func Apply(config *rest.Config, inputFilename, namespace string, valueFilenames []string) error {

	data, err := combineValues(valueFilenames, false)
	if err != nil {
		return fmt.Errorf("error reading in values data: %s", err)
	}
	data["namespace"] = namespace

	f, err := os.Stat(inputFilename)
	if err != nil {
		return fmt.Errorf("could not stat file: %s", err)
	}

	b, err := render(f, inputFilename, data)
	if err != nil {
		return fmt.Errorf("could not render template: %s", err)
	}

	return createOrUpdateResource(b, namespace, config)
}

func createOrUpdateResource(b []byte, namespace string, config *rest.Config) error {
	ctx := context.TODO()

	obj := unstructured.Unstructured{}
	err := yaml.Unmarshal(b, &obj)
	if err != nil {
		return fmt.Errorf("ERROR: could not unmarshal resource: %s", err)
	}

	if obj.IsList() {
		err := obj.EachListItem(func(item runtime.Object) error {
			b, err := json.Marshal(item)
			if err != nil {
				return err
			}
			return createOrUpdateResource(b, namespace, config)
		})
		if err != nil {
			return err
		}
		return nil
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "List" {
		// Check if gvk kind is a list, these should be ignored after checking if there are list items with 0 items
		return nil
	}

	var dynamicClient dynamic.ResourceInterface
	namespaceableResourceClient, isNamespaced, err := getDynamicClientOnKind(gvk.GroupVersion().String(), gvk.Kind, config)
	if err != nil {
		return fmt.Errorf("ERROR: could not get a client to handle resource: %s", err)
	}
	if isNamespaced {
		if obj.GetNamespace() != "" {
			namespace = obj.GetNamespace()
		} else {
			obj.SetNamespace(namespace)
		}
		dynamicClient = namespaceableResourceClient.Namespace(namespace)
	} else {
		dynamicClient = namespaceableResourceClient
	}

	obj.SetSelfLink("")
	obj.SetResourceVersion("")
	obj.SetUID("")
	obj.SetOwnerReferences([]metav1.OwnerReference{}) // TODO fix to original tf

	_, err = dynamicClient.Create(ctx, &obj, metav1.CreateOptions{})
	if err != nil {
		if kerrors.IsAlreadyExists(err) {
			log.Printf("%s '%s/%s' already exists. Updating resource", gvk.Kind, namespace, obj.GetName())
			// Get a clean mergable object
			b, err := makeNewPatchableData(&obj)
			if err != nil {
				return fmt.Errorf("could not marshal resource '%s/%s': %s", namespace, obj.GetName(), err)
			}
			_, err = dynamicClient.Patch(ctx, obj.GetName(), types.StrategicMergePatchType, b, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("ERROR: could not patch %s '%s/%s': %s", gvk.Kind, namespace, obj.GetName(), err)
			}
			log.Printf("%s '%s/%s' has been updated", gvk.Kind, namespace, obj.GetName())
		} else {
			return fmt.Errorf("ERROR: could not create %s '%s/%s': %s", gvk.Kind, namespace, obj.GetName(), err)
		}
	} else {
		log.Printf("%s '%s/%s' has been created", gvk.Kind, namespace, obj.GetName())
	}
	return nil
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

func makeNewPatchableData(obj *unstructured.Unstructured) ([]byte, error) {
	gvks, _, err := scheme.Scheme.ObjectKinds(obj)
	if err != nil {
		return nil, err
	}
	if len(gvks) == 0 {
		return nil, fmt.Errorf("No gvks identified")
	}
	obj.SetGroupVersionKind(gvks[0])

	buf := bytes.NewBuffer([]byte{})
	k8sjson.NewSerializer(k8sjson.DefaultMetaFactory, runtime.NewScheme(), runtime.NewScheme(), true).Encode(obj, buf)
	return buf.Bytes(), nil
}

// render fills in a template with data from values. Values can contain
// values and render is called recursively until all values are filled.
//
// This function cannot be used to generate another template since any
// string perceived to be a template function (eg "{{" strings) will attempt to
// be filled in by this function.
func render(file os.FileInfo, templateFile string, data map[string]interface{}) ([]byte, error) {
	fmap := sprig.TxtFuncMap()                   // sprig mapper for text template
	tpl := template.New(file.Name()).Funcs(fmap) // setup sprig funcs for template
	tpl, err := tpl.ParseFiles(templateFile)
	if err != nil {
		return nil, err
	}

	tmp, err := ioutil.TempFile(tmpdir(), "tmp_")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name()) // clean up

	err = tpl.Execute(tmp, data) // write to new template file
	if err != nil {
		return nil, err
	}

	return ioutil.ReadFile(tmp.Name()) // read the new file (again?)
}

func tmpdir() string {
	t := os.TempDir()
	_, err := os.Stat(t)
	if err != nil {
		return "."
	}
	return t
}

// combineValues merges multiple value files into a single data object. The
// files are read in order they are passed into the function. This means that
// the values in the next file over-writes any previous value.
//
// Currently only supports YAML formatted value files.
func combineValues(filesToMerge []string, recurseArrays bool) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	for _, file := range filesToMerge {
		d, err := readValues(file)
		if err != nil {
			return data, err
		}
		data = mergeMaps(data, d, recurseArrays)
	}
	return data, nil
}

func readValues(path string) (map[string]interface{}, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to  read values file: %s", path)
	}
	data := make(map[string]interface{}, 0)
	if err := yaml.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("unable decode the values content")
	}
	return data, nil

}

// fileContains searchs a file line by line for the matching substring. Returns
// true if there's a match.
func fileContains(path, substring string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Splits on newlines by default. https://golang.org/pkg/bufio/#Scanner.Scan
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), substring) {
			return true, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// mergeMaps takes two map types and merges the two into a single map. The
// second map, d2, over-writes any data from the first map, d1.
//
// In the event that the value of a map is also a map, this function is called
// recursively to do a merge between those two maps.
func mergeMaps(d1, d2 map[string]interface{}, recurseArrays bool) map[string]interface{} {
	for k, v := range d2 {
		if m, ok := v.(map[string]interface{}); ok {
			// v is a map (m), go deeper
			if d1[k] != nil {
				// if d1 contains "k", check that it's a map
				if n, ok := d1[k].(map[string]interface{}); ok {
					// d1[k] is a map (n), merge (n) and (m)
					mergeMaps(n, m, recurseArrays)
				} else {
					// the value of the key is a different type than before. Go ahead
					// and replace the type
					d1[k] = v
				}
			} else {
				// d1 does not contain "k", create it now
				d1[k] = v
			}
		} else if m, ok := v.([]interface{}); ok && recurseArrays {
			// v is an array, append the array
			if d1[k] != nil {
				// if d1 containes "k", check that it's an array
				if n, ok := d1[k].([]interface{}); ok {
					d1[k] = append(n, m...)
				} else {
					d1[k] = v
				}
			} else {
				d1[k] = v
			}

		} else {
			// v is not a map, update the value
			d1[k] = v
		}
	}
	return d1
}

func KubernetesConfig(kubeconfigPath string) *rest.Config {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		log.Fatal("Failed to get config for clientset")
	}
	return config
}
