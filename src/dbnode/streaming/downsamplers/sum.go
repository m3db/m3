package downsamplers

var _ Downsampler = (*sum)(nil)

type sum struct {
	sum float64
}

func NewSumDownsampler() Downsampler {
	return &sum{}
}

func (d *sum) Accept(value float64) {
	d.sum += value
}

func (d *sum) Emit() float64 {
	result := d.sum
	d.sum = 0
	return result
}
