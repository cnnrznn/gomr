package gomr

import (
	"fmt"

	ftp "github.com/cnnrznn/goftp"
	"github.com/cnnrznn/gomr/store"
)

func (j *Job) doMap() error {
	// create stores for each reducer in cluster
	midStores := make([]store.Store, j.Cluster.Size())
	for i := 0; i < j.Cluster.Size(); i++ {
		midStores[i] = &store.FileStore{Filename: makeFN(j.Cluster.Self, i, j.Name)}
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
	// start server
	go j.receive()
	j.sendall()

	// for each pier, fetch the intermediate data meant for us
	return fmt.Errorf("Not implemented")
}

func (j *Job) receive() {
	// TODO create folder for received files

	for i := range j.Cluster.Nodes {
		ftp.ReceiveFile(ftp.Option{
			Addr:     fmt.Sprintf(":%v", 3333),
			Filename: makeFN(i, j.Cluster.Self, j.Name),
		})
	}
}

func (j *Job) sendall() {
	for i, node := range j.Cluster.Nodes {
		ftp.SendFile(ftp.Option{
			Addr:     fmt.Sprintf("%v:3333", node),
			Filename: makeFN(j.Cluster.Self, i, j.Name),
		})
	}
}

func (j *Job) doReduce() error {
	// feed the intermediate data generated on this machine and from every pier to tier1 reducer
	return fmt.Errorf("Not implemented")
}
