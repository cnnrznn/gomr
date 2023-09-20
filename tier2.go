package gomr

import (
	"fmt"

	ftp "github.com/cnnrznn/goftp"
	"github.com/cnnrznn/gomr/store"
)

func (j *Job) doReduce() error {
	inputs := make([]store.Store, j.Cluster.Size())
	for i := range inputs {
		inputs[i] = &store.FileStore{Filename: fmt.Sprintf("%v/%v", DIR_POSTSHUFFLE, makeFN(i, j.Cluster.Self, j.Name))}
		err := inputs[i].Init()
		if err != nil {
			return err
		}
		defer inputs[i].Close()
	}

	output := &store.FileStore{Filename: fmt.Sprintf("%v-out-%v", j.Name, j.Cluster.Self)}
	err := output.Init()
	if err != nil {
		return err
	}
	defer output.Close()

	return j.reduce(inputs, output)
}

func (j *Job) doMap() error {
	// create stores for each reducer in cluster
	midStores := make([]store.Store, j.Cluster.Size())
	for i := range midStores {
		midStores[i] = &store.FileStore{Filename: fmt.Sprintf("%v/%v", DIR_PRESHUFFLE, makeFN(j.Cluster.Self, i, j.Name))}
		err := midStores[i].Init()
		if err != nil {
			return err
		}
		defer midStores[i].Close()
	}

	// if an input is local, process it
	localStores := []store.Store{}

	for _, config := range j.Inputs {
		url := config.Url()
		host := url.Hostname()
		path := url.Path

		switch host {
		case "localhost", "127.0.0.1", "", j.Cluster.Nodes[j.Cluster.Self]:
			st := &store.FileStore{Filename: path}
			err := st.Init()
			if err != nil {
				return err
			}
			defer st.Close()

			localStores = append(localStores, st)
		}
	}

	return j.transform(localStores, midStores)
}

func (j *Job) doShuffle() error {
	errChan := make(chan error)

	go func() {
		err := j.receive()
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	err := j.sendall()
	if err != nil {
		return err
	}

	for err := range errChan {
		return err
	}

	return nil
}

func (j *Job) receive() error {
	for i := range j.Cluster.Nodes {
		err := ftp.ReceiveFile(ftp.Option{
			Addr:     fmt.Sprintf(":%v", 3333),
			Filename: fmt.Sprintf("%v/%v", DIR_POSTSHUFFLE, makeFN(i, j.Cluster.Self, j.Name)),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) sendall() error {
	for i, node := range j.Cluster.Nodes {
		opt := ftp.Option{
			Addr:     fmt.Sprintf("%v:3333", node),
			Filename: fmt.Sprintf("%v/%v", DIR_PRESHUFFLE, makeFN(j.Cluster.Self, i, j.Name)),
			Retries:  3,
		}
		err := ftp.SendFile(opt)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeFN(
	from, to int,
	name string,
) string {
	return fmt.Sprintf("%v-%v-%v", name, from, to)
}
