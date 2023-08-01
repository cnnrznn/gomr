package gomr

import (
	"fmt"

	"github.com/cnnrznn/gomr/store"
)

func (j Job) MapReduce() error {
	// initialize data stores and storage server

	// Transform all stores on our machine
	// Shuffle outputs to appropriate machine
	// Reduce all stores to a given Key

	// 1. Open all stores on this machine
	// 2. Create mid stores, 1 for each reducer machine
	// 3. Run Transform step

	shuffle := make([]store.Store, len(j.Cluster.Nodes))
	for i := range shuffle {
		store, err := store.Init(store.Config{
			URL: fmt.Sprintf("file:///tmp/shuffle-%v.txt", i),
		})
		if err != nil {
			return err
		}

		shuffle[i] = store
	}

	return nil
}
