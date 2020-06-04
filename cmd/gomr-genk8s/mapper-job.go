package main

var mapperJobStr = `
{{ $inprefix := .inprefix }}
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
          args: ["-input={{ $inprefix }}.{{ $i }}"]
          image: gomr
          ports:
            - name: mr-port
              containerPort: 3000
---
{{ end }}
`
