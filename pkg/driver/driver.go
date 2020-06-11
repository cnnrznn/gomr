package driver

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"
)

// Driver determines the name of the job within the Kubernetes namespace and the
// parallelism of the job. The number of mappers and reducers will be equal to
// NProcs.
type Driver struct {
	Name   string
	NProcs int
}

// NewDriver allocates a Driver struct with the given app name and parallelism.
func NewDriver(name string, nprocs int) *Driver {
	return &Driver{
		Name:   name,
		NProcs: nprocs,
	}
}

// Run Executes a GoMR job on a cluster, monitoring for and re-running on
// failure.
func (d *Driver) Run(image, input, output string) {
	client := getClient()
	_, _, rss := makeJobs(image, input, output, d.NProcs)
	//mjs, rjs, rss := makeJobs(image, input, output, d.NProcs)
	serviceRes := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}

	for _, j := range rss {
		bs, _ := json.MarshalIndent(j, "", " ")
		fmt.Println(string(bs))
		result, err := client.Resource(serviceRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
		if err != nil {
			log.Panic(err)
		}
		log.Println("Created service: ", result.GetName())

		err = client.Resource(serviceRes).Namespace("default").Delete(context.TODO(), result.GetName(), metav1.DeleteOptions{})
		if err != nil {
			log.Panic(err)
		}
		log.Println("Deleted service")
	}

	//for _, j := range rjs {
	//	bs, _ := json.MarshalIndent(j, "", " ")
	//	fmt.Println(string(bs))
	//}

	//for _, j := range mjs {
	//	bs, _ := json.MarshalIndent(j, "", " ")
	//	fmt.Println(string(bs))
	//}
}

func getClient() dynamic.Interface {
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return clientset
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
