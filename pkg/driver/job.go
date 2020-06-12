package driver

import (
	"crypto/rand"
	"fmt"
	"log"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func makeJobs(image, input, output string, nprocs int) (
	mjs, rjs, rss []unstructured.Unstructured,
) {
	// TODO make random string for unique job identifier
	guid := make([]byte, 16)
	if _, err := rand.Read(guid); err != nil {
		log.Panic(err)
	}
	name := fmt.Sprintf("%v-%x", image, guid)

	mjs = makeMapJobs(image, name, input, nprocs)
	rjs = makeReduceJobs(image, name, output, nprocs)
	rss = makeReduceServices(name, nprocs)

	return
}

func reducerList(name string, nprocs int) string {
	reducers := []string{}
	for i := 1; i <= nprocs; i++ {
		reducers = append(reducers, fmt.Sprintf("%v-reducer-%v:3000", name, i))
	}
	return strings.Join(reducers, ",")
}

func makeMapJobs(image, name, input string, nprocs int) []unstructured.Unstructured {
	mapJobs := []unstructured.Unstructured{}
	for i := 1; i <= nprocs; i++ {
		mapJobs = append(mapJobs, unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "batch/v1",
				"kind":       "Job",
				"metadata": map[string]interface{}{
					"name": fmt.Sprintf("%v-%v", name, i),
				},
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"volumes": []map[string]interface{}{
								{
									"name": "gomr-pv-storage",
									"persistentVolumeClaim": map[string]interface{}{
										"claimName": "gomr-pv-claim",
									},
								},
							},
							"restartPolicy": "Never",
							"containers": []map[string]interface{}{
								{
									"imagePullPolicy": "Never",
									"name":            name,
									"args": []string{fmt.Sprintf("-input=%v.%v", input, i),
										fmt.Sprintf("-nmappers=%v", nprocs),
										fmt.Sprintf("-reducers=%v", reducerList(name, nprocs)),
									},
									"image": image,
									"volumeMounts": []map[string]interface{}{
										{
											"mountPath": "/data",
											"name":      "gomr-pv-storage",
										},
									},
									"ports": []map[string]interface{}{
										{
											"name":          "mr-port",
											"containerPort": 3000,
										},
									},
								},
							},
						},
					},
				},
			},
		})
	}
	return mapJobs
}

func makeReduceJobs(image, name, output string, nprocs int) []unstructured.Unstructured {
	reduceJobs := []unstructured.Unstructured{}
	for i := 1; i <= nprocs; i++ {
		podName := fmt.Sprintf("%v-reducer-%v", name, i)
		reduceJobs = append(reduceJobs, unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "batch/v1",
				"kind":       "Job",
				"metadata": map[string]interface{}{
					"name": podName,
				},
				"spec": map[string]interface{}{
					"manualSelector": true,
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": podName,
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": podName,
							},
						},
						"spec": map[string]interface{}{
							"volumes": []map[string]interface{}{
								{
									"name": "gomr-pv-storage",
									"persistentVolumeClaim": map[string]interface{}{
										"claimName": "gomr-pv-claim",
									},
								},
							},
							"restartPolicy": "Never",
							"hostname":      podName,
							"containers": []map[string]interface{}{
								{
									"imagePullPolicy": "Never",
									"name":            podName,
									"args": []string{
										"-role=1",
										fmt.Sprintf("-output=%v.%v", output, i),
										fmt.Sprintf("-id=%v", i-1),
										fmt.Sprintf("-nmappers=%v", nprocs),
										fmt.Sprintf("-reducers=%v", reducerList(name, nprocs)),
									},
									"image": image,
									"volumeMounts": []map[string]interface{}{
										{
											"mountPath": "/data",
											"name":      "gomr-pv-storage",
										},
									},
									"ports": []map[string]interface{}{
										{
											"name":          "mr-port",
											"containerPort": 3000,
										},
									},
								},
							},
						},
					},
				},
			},
		})
	}
	return reduceJobs
}

func makeReduceServices(name string, nprocs int) []unstructured.Unstructured {
	reducerServices := []unstructured.Unstructured{}
	for i := 1; i <= nprocs; i++ {
		podName := fmt.Sprintf("%v-reducer-%v", name, i)
		reducerServices = append(reducerServices, unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata": map[string]interface{}{
					"name": podName,
				},
				"spec": map[string]interface{}{
					"selector": map[string]interface{}{
						"app": podName,
					},
					"ports": []map[string]interface{}{
						{
							"port":       3000,
							"targetPort": "mr-port",
						},
					},
				},
			},
		})
	}
	return reducerServices
}
