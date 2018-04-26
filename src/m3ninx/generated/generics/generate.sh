#!/bin/bash

if [ -z $GOPATH ]; then
    echo 'GOPATH is not set'
    exit 1
fi

GENERIC_MAP_PATH=${GOPATH}/src/github.com/m3db/m3ninx/vendor/github.com/m3db/m3x/generics/hashmap
GENERIC_MAP_IMPL=${GENERIC_MAP_PATH}/map.go

if [ ! -f "$GENERIC_MAP_IMPL" ]; then
    echo "${GENERIC_MAP_IMPL} does not exist"
    exit 1
fi

GENERATED_PATH=${GOPATH}/src/github.com/m3db/m3ninx/index/segment/mem
if [ ! -d "$GENERATED_PATH" ]; then
    echo "${GENERATED_PATH} does not exist"
    exit 1
fi

# NB: We use (genny)[1] to combat the lack of generics in Go. It allows us
# to declare templat-ized versions of code, and specialize using code
# generation.  We have a generic HashMap<K,V> implementation in `m3x`,
# with two template variables (KeyType and ValueType), along with required
# functors (HashFn, EqualsFn, ...). Below, we generate a few specialized
# versions required in m3ninx today.
#
# If we need to generate other specialized map implementations, or other
# specialized datastructures, we should add targets similar to the ones below.
#
# [1]: https://github.com/cheekybits/genny

mkdir -p $GENERATED_PATH/postingsgen
cat $GENERIC_MAP_IMPL                                                  \
| genny -out=${GENERATED_PATH}/postingsgen/map_gen.go            \
-pkg=postingsgen gen "KeyType=[]byte ValueType=postings.MutableList"

mkdir -p $GENERATED_PATH/fieldsgen
cat $GENERIC_MAP_IMPL                                                      \
| genny -out=${GENERATED_PATH}/fieldsgen/map_gen.go                  \
-pkg=fieldsgen gen "KeyType=[]byte ValueType=*postingsgen.ConcurrentMap"

mkdir -p $GENERATED_PATH/idsgen
cat $GENERIC_MAP_IMPL                                   \
| genny -out=${GENERATED_PATH}/idsgen/map_gen.go  \
-pkg=idsgen gen "KeyType=[]byte ValueType=struct{}"
