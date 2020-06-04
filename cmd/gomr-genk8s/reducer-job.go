package main

var reducerJobStr = `
{{ $name := .name }}
{{ $outprefix := .outprefix }}
{{range $i := iter 1 .nreducers }}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $name }}-reducer-{{ $i }}
spec:
  manualSelector: true
  selector:
    matchLabels:
      app: {{ $name }}-reducer-{{ $i }}
  template:
    metadata:
      labels:
        app: {{ $name }}-reducer-{{ $i }}
    spec:
      hostname: {{ $name }}-reducer-{{ $i }}
      restartPolicy: "Never"
      containers:
        - name: {{ $name }}-reducer-{{ $i }}
          args: ["-role=1", "-id={{ dec $i }}", "-output={{ $outprefix }}.{{ $i }}"]
          image: gomr
          ports:
            - name: mr-port
              containerPort: 3000
---
{{ end }}
`
