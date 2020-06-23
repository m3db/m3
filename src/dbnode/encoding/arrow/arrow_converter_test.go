package arrow

import (
	"testing"
)

func TestConvert(t *testing.T) {
	iter := newSeriesIter(10, 0, 10)
	convert(iter)
}
