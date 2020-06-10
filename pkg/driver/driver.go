package driver

import (
	"flag"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	//
	// Uncomment to load all auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/openstack"
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
func (d *Driver) Run(image string) {

}

func getClient() *kubernetes.Clientset {
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
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return clientset

	//for {
	//	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	//	if err != nil {
	//		panic(err.Error())
	//	}
	//	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

	//	// Examples for error handling:
	//	// - Use helper functions like e.g. errors.IsNotFound()
	//	// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
	//	namespace := "default"
	//	pod := "example-xxxxx"
	//	_, err = clientset.CoreV1().Pods(namespace).Get(context.TODO(), pod, metav1.GetOptions{})
	//	if errors.IsNotFound(err) {
	//		fmt.Printf("Pod %s in namespace %s not found\n", pod, namespace)
	//	} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
	//		fmt.Printf("Error getting pod %s in namespace %s: %v\n",
	//			pod, namespace, statusError.ErrStatus.Message)
	//	} else if err != nil {
	//		panic(err.Error())
	//	} else {
	//		fmt.Printf("Found pod %s in namespace %s\n", pod, namespace)
	//	}

	//	time.Sleep(10 * time.Second)
	//}
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
