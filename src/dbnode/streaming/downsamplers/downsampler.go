package downsamplers

type Downsampler interface {
	Accept(value float64)
	Emit() float64
}
