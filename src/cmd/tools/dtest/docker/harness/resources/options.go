package resources

type dockerImage struct {
	name string
	tag  string
}

type setupOptions struct {
	dbNodeImage      dockerImage
	coordinatorImage dockerImage
}

type SetupOptions interface {
	apply(*setupOptions)
}

type dbnodeImageNameOption struct {
	image dockerImage
}

func (c dbnodeImageNameOption) apply(opts *setupOptions) {
	opts.dbNodeImage = c.image
}

func WithDBNodeImage(name, tag string) SetupOptions {
	return dbnodeImageNameOption{dockerImage{name, tag}}
}

type coordinatorImageNameOption struct {
	image dockerImage
}

func (c coordinatorImageNameOption) apply(opts *setupOptions) {
	opts.coordinatorImage = c.image
}

func WithCoordinatorImage(name, tag string) SetupOptions {
	return coordinatorImageNameOption{dockerImage{name, tag}}
}
