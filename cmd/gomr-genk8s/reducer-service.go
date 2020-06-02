package main

var reducerServiceStr = `
{{ $name := .name }}
{{range $i := iter 1 .nreducers }}
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}-reducer-{{ $i }}
spec:
  selector:
    app: {{ $name }}-reducer-{{ $i }}
  ports:
    - port: 3000
      targetPort: mr-port
---
{{ end }}
`
