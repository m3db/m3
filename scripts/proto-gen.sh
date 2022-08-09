#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="${DIR}/.."

# include portable readlink
source "${DIR}/realpath.sh"

# paranoia, ftw
set -e
PROTOC_IMAGE_VERSION=${PROTOC_IMAGE_VERSION:-"znly/protoc:0.2.0"}

# ensure docker is running
docker run --rm hello-world >/dev/null

UID_FLAGS="-u $(id -u)"
if [[ -n "$BUILDKITE" ]]; then
	UID_FLAGS="-u root"
fi

PROTO_SRC=$1
# Find all protobuf files under ${PROTO_SRC}
for i in "${PROTO_SRC}"/*; do
	if ! [ -d $i ]; then
		continue
	fi

	if ls $i/*.proto > /dev/null 2>&1; then

    # Find all .proto files in this subdirectory, and return the full import path to them. The import path is:
    # github.com/m3db/m3/<path_from_root>
    # compute their path relative to the repository root.
    proto_files="$(find $i -name \*.proto | while read -r filename; do
      full_path="$(realpath ${filename})"

      # Relativize it to the root
      root_full_path="$(realpath "${ROOT}")"

      # Bash substitution:
      # https://stackoverflow.com/questions/13210880/replace-one-substring-for-another-string-in-shell-script
      path_from_root="${full_path/${root_full_path}}"
      # Finally
      echo "github.com/m3db/m3${path_from_root}"
    done)"

		echo "generating from ${proto_files}"
		# need the additional m3db_path mount in docker because it's a symlink on the CI.
		m3db_path=$(realpath "${ROOT}")
    resolve_protos="Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types"

		docker run --rm -w /src \
		-v "${ROOT}/src:/src" \
		-v ${m3db_path}:/src/github.com/m3db/m3 \
		$UID_FLAGS $PROTOC_IMAGE_VERSION \
			 --gogofaster_out=${resolve_protos},plugins=grpc:/src \
			 -I/src \
			 ${proto_files}

	fi
done
