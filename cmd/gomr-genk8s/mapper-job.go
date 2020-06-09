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
      volumes:
        - name: task-pv-storage
          persistentVolumeClaim:
            claimName: task-pv-claim
      restartPolicy: "Never"
      containers:
        - name: mapper-{{ $i }}
          args: ["-input={{ $inprefix }}.{{ $i }}"]
          image: gomr
          volumeMounts:
            - mountPath: "/data"
              name: task-pv-storage
          ports:
            - name: mr-port
              containerPort: 3000
---
{{ end }}
`
