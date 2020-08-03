package downsamplers

var _ Downsampler = (*count)(nil)

type count struct {
	value uint32
}

func NewCountDownsampler() Downsampler {
	return &count{}
}

func (d *count) Accept(float64) {
	d.value++
}

func (d *count) Emit() float64 {
	result := d.value
	d.value = 0
	return float64(result)
}
