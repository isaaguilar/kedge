# kedge

Install or update a kubernetes manifest by passing in the a Kubernetes manifest. The manifest can be a `go-templates`
file. For example, a resource can specify `namespace: "{{ .namespace }}"` which will be filled in by the namespace value.

**Example:**

```go
package main

import (
	"github.com/isaaguilar/kedge"
	"os"
)

func main() {
	manifest := os.Args[1]
	kedge.Apply(kedge.KubernetesConfig(os.Getenv("KUBECONFIG")), manifest, "default", []string{})
}
```