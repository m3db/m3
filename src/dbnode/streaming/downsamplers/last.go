package downsamplers

var _ Downsampler = (*lastValue)(nil)

type lastValue struct {
	value float64
}

func NewLastValueDownsampler() Downsampler {
	return &lastValue{}
}

func (d *lastValue) Accept(value float64) {
	d.value = value
}

func (d *lastValue) Emit() float64 {
	return d.value
}
