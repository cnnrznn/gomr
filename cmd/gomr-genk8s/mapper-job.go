package main

var mapperJobStr = `
{{ $prefix := .prefix }}
{{range $i := iter 1 .nmappers }}
apiVersion: batch/v1
kind: Job
metadata:
  name: mapper-{{ $i }}
spec:
  template:
    spec:
      restartPolicy: "Never"
      containers:
        - name: mapper-{{ $i }}
          args: ["-input={{ $prefix }}.{{ $i }}"]
          image: gomr
          ports:
            - name: mr-port
              containerPort: 3000
---
{{ end }}
`
