package driver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func makeJobs(image, input, output string, nprocs int) (
	mjs, rjs, rss []unstructured.Unstructured,
) {
	// TODO make random string for unique job identifier
	name := image

	mjs = makeMapJobs(name, input, nprocs)
	rjs = makeReduceJobs(name, output, nprocs)
	rss = makeReduceServices(name, nprocs)

	return
}

func makeMapJobs(name, input string, par int) []unstructured.Unstructured {
	mapJobs := []unstructured.Unstructured{}
	for i := 1; i <= par; i++ {
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
									"name": name,
									"args": []string{fmt.Sprintf("-input=%v", input),
										fmt.Sprintf("-nmappers=%v", par),
										//fmt.Sprintf("-reducers=%v", reducers),
									},
									"image": name,
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

func makeReduceJobs(image, output string, par int) []unstructured.Unstructured {
	reduceJobs := []unstructured.Unstructured{}
	for i := 1; i <= par; i++ {
		podName := fmt.Sprintf("%v-reducer-%v", image, i)
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
									"name": podName,
									"args": []string{fmt.Sprintf("-output=%v", output),
										"-role=1",
										fmt.Sprintf("-id=%v", i),
										fmt.Sprintf("-nmappers=%v", par),
										//fmt.Sprintf("-reducers=%v", reducers),
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

func makeReduceServices(image string, par int) []unstructured.Unstructured {
	reducerServices := []unstructured.Unstructured{}
	for i := 1; i <= par; i++ {
		podName := fmt.Sprintf("%v-reducer-%v", image, i)
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
