package gomr

func (j *Job) MapReduce() error {
	err := j.doMap()
	if err != nil {
		return err
	}
	//j.doShuffle()
	//j.doReduce()

	return nil
}
