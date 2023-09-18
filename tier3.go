package gomr

import (
	"os"
)

func (j *Job) MapReduce() error {
	setup()
	defer destroy()

	err := j.doMap()
	if err != nil {
		return err
	}

	err = j.doShuffle()
	if err != nil {
		return err
	}

	err = j.doReduce()
	if err != nil {
		return err
	}

	return nil
}

const (
	DIR_PRESHUFFLE  = "preshuffle"
	DIR_POSTSHUFFLE = "postshuffle"
)

func setup() {
	os.Mkdir(DIR_PRESHUFFLE, 0755)
	os.Mkdir(DIR_POSTSHUFFLE, 0755)
}

func destroy() {
	os.RemoveAll(DIR_PRESHUFFLE)
	os.RemoveAll(DIR_POSTSHUFFLE)
}
