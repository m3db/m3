package ts

// Memset is a faster way to initialize a float64 array.
/** Inspired from https://github.com/tmthrgd/go-memset. Can't use that library here directly since the library works on byte interface.
 0 case is optimized due to https://github.com/golang/go/issues/5373 but for non zero case, we use the copy() optimization.
BenchmarkMemsetZeroValues-4              1000000              1344 ns/op               0 B/op          0 allocs/op
BenchmarkLoopZeroValues-4                 500000              3217 ns/op               0 B/op          0 allocs/op
BenchmarkMemsetNonZeroValues-4           1000000              1537 ns/op               0 B/op          0 allocs/op
BenchmarkLoopNonZeroValues-4              500000              3236 ns/op               0 B/op          0 allocs/op
**/
func Memset(data []float64, value float64) {
	if value == 0 {
		for i := range data {
			data[i] = 0
		}
	} else if len(data) != 0 {
		data[0] = value

		for i := 1; i < len(data); i *= 2 {
			copy(data[i:], data[:i])
		}
	}
}
