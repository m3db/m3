install-vendor: install-glide
	@echo Installing glide deps
	glide --debug install

install-glide:
		@which glide > /dev/null || (go get -u github.com/Masterminds/glide && cd $(GOPATH)/src/github.com/Masterminds/glide && git checkout v0.12.3 && go install)
		@glide -version > /dev/null || echo "Glide install failed"

install-ci:
	make install-vendor

