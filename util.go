package gomr

import "fmt"

func makeFN(
	from, to int,
	name string,
) string {
	return fmt.Sprintf("%v-%v-%v", name, from, to)
}
