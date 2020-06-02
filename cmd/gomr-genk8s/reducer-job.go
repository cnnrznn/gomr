package main

var reducerJobStr = `
{{range $i := iter 1 .nreducers }}
apiVersion: batch/v1
kind: Job
metadata:
  name: reducer-{{ $i }}
spec:
  manualSelector: true
  selector:
    matchLabels:
      app: reducer-{{ $i }}
  template:
    metadata:
      labels:
        app: reducer-{{ $i }}
    spec:
      hostname: reducer-{{ $i }}
      restartPolicy: "OnFailure"
      containers:
      - name: reducer-{{ $i }}
        args: ["-role=1", "-id={{ dec $i }}"]
        image: gomr
        ports:
        - name: mr-port
		  containerPort: 3000
---
{{ end }}
`
