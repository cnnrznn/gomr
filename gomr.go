package gomr

import "github.com/cnnrznn/gomr/store"

func (j Job) MapReduce() error {
	// initialize data stores and storage server
	storage := store.NewService()
	go storage.ServeMapData()

	return nil
}
