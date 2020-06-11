package driver

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

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
	mjs, rjs, rss := makeJobs(image, input, output, d.NProcs)
	serviceRes := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	jobRes := schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}

	for _, j := range rss {
		result, err := client.Resource(serviceRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
		if err != nil {
			log.Panic(err)
		}
		log.Println("Created service: ", result.GetName())

		defer func(name string) {
			err = client.Resource(serviceRes).Namespace("default").Delete(context.TODO(), name, metav1.DeleteOptions{})
			if err != nil {
				log.Println(err)
			}
			log.Println("Deleted service")
		}(result.GetName())
	}

	for _, j := range rjs {
		result, err := client.Resource(jobRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
		if err != nil {
			log.Panic(err)
		}
		log.Println("Created job: ", result.GetName())
		defer cleanupJob(client, result.GetName())
	}

	for _, j := range mjs {
		result, err := client.Resource(jobRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
		if err != nil {
			log.Panic(err)
		}
		log.Println("Created job: ", result.GetName())
		defer cleanupJob(client, result.GetName())
	}

	// launch informer and listen for failure/completion events
	time.Sleep(40 * time.Second)
}

func cleanupJob(client dynamic.Interface, name string) {
	jobRes := schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
	deletePolicy := metav1.DeletePropagationForeground
	err := client.Resource(jobRes).Namespace("default").Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil {
		log.Println(err)
	}
	log.Println("Deleted service")
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
