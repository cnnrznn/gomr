package gomr

// Chain two Jobs together by feeding the output from one
// Job to the inputs of another.
func Chain(in <-chan interface{}, outs []chan interface{}) {
	go func() {
		i := 0
		for e := range in {
			outs[i] <- e
			i = (i + 1) % len(outs)
		}

		for _, ch := range outs {
			close(ch)
		}
	}()
}
