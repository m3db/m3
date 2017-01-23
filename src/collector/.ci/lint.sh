#!/bin/bash

! golint ./... | egrep -v $(cat .excludelint | sed -e 's/^/ -e /' | sed -e 's/(//' | sed -e 's/)//'  | tr -d '\n')
