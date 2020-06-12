package driver

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	serviceRes := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	jobRes := schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
	svcNames := make([]string, d.NProcs)
	jobNames := make([]string, d.NProcs*2)

	watch, err := client.Resource(jobRes).Namespace("default").Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Panic(err)
	}

	for {
		mjs, rjs, rss := makeJobs(image, input, output, d.NProcs)

		for i, j := range rss {
			result, err := client.Resource(serviceRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
			if err != nil {
				log.Panic(err)
			}
			log.Println("Created service: ", result.GetName())
			svcNames[i] = result.GetName()
		}

		for i, j := range rjs {
			result, err := client.Resource(jobRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
			if err != nil {
				log.Panic(err)
			}
			log.Println("Created job: ", result.GetName())
			jobNames[i] = result.GetName()
		}

		for i, j := range mjs {
			result, err := client.Resource(jobRes).Namespace("default").Create(context.TODO(), &j, metav1.CreateOptions{})
			if err != nil {
				log.Panic(err)
			}
			log.Println("Created job: ", result.GetName())
			jobNames[d.NProcs+i] = result.GetName()
		}

		success := 0
		failure := 0
		for success+failure < d.NProcs*2 {
			event := <-watch.ResultChan()
			job := event.Object.(*unstructured.Unstructured)
			status := job.Object["status"].(map[string]interface{})
			log.Println(status)
			if _, ok := status["succeeded"]; ok {
				success++
			}
			if _, ok := status["failed"]; ok {
				failure++
			}
		}

		for _, n := range jobNames {
			cleanupJob(client, n)
		}
		for _, n := range svcNames {
			cleanupSvc(client, n)
		}

		if success == d.NProcs*2 {
			break
		}
	}
}

func cleanupJob(client dynamic.Interface, name string) {
	jobRes := schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
	deletePolicy := metav1.DeletePropagationBackground
	err := client.Resource(jobRes).Namespace("default").Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Deleted job %v\n", name)
}

func cleanupSvc(client dynamic.Interface, name string) {
	serviceRes := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	err := client.Resource(serviceRes).Namespace("default").Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Deleted service %v\n", name)
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
