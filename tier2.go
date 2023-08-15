package gomr

import (
	"fmt"

	"github.com/cnnrznn/gomr/store"
)

func (j *Job) doMap() error {
	// create stores for each reducer in cluster
	midStores := make([]store.Store, j.Cluster.Size())
	for i := 0; i < j.Cluster.Size(); i++ {
		midStores[i] = &store.FileStore{Filename: fmt.Sprintf("%v-%v-%v", j.Name, j.Cluster.Self, i)}
		err := midStores[i].Init()
		if err != nil {
			return err
		}
	}

	// if an input is local, process it
	localStores := []store.Store{}

	for _, config := range j.Inputs {
		url := config.Url()
		host := url.Hostname()
		path := url.Path

		switch host {
		case "localhost", "127.0.0.1", "":
			st := &store.FileStore{Filename: path}
			err := st.Init()
			if err != nil {
				return err
			}

			localStores = append(localStores, st)
		}
	}

	// push processing to lower tier
	err := j.transform(localStores, midStores)
	if err != nil {
		return err
	}

	for _, st := range localStores {
		st.Close()
	}
	for _, st := range midStores {
		st.Close()
	}

	return nil
}

func (j *Job) doShuffle() error {
	return fmt.Errorf("Not implemented")
}

func (j *Job) doReduce() error {
	return fmt.Errorf("Not implemented")
}
