package main

var reducerServiceStr = `
{{range $i := iter 1 .nreducers }}
apiVersion: v1
kind: Service
metadata:
  name: reducer-{{ $i }}
spec:
  selector:
    app: reducer-{{ $i }}
  ports:
  - port: 3000
    targetPort: mr-port
---
{{ end }}
`
