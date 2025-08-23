#!/bin/bash

# Auto-detecting protoc generation script
# Uses native protoc if available, falls back to Docker with platform detection

set -e

# Check if native protoc and plugins are available
check_native_protoc() {
    command -v protoc >/dev/null 2>&1 && command -v protoc-gen-gogofaster >/dev/null 2>&1
}

# Detect architecture for Docker platform selection
detect_platform() {
    local arch=$(uname -m)
    case $arch in
        x86_64)
            echo "linux/amd64"
            ;;
        arm64|aarch64)
            echo "linux/arm64"
            ;;
        *)
            echo "linux/amd64"  # fallback to amd64
            ;;
    esac
}

PROTO_SRC=$1

if check_native_protoc; then
    echo "Using native protoc (recommended for Apple Silicon)"
    # Use native protoc generation with dependency-aware ordering
    
    # Define compilation order to handle dependencies
    # policypb must be compiled before metricpb (metricpb imports policypb.RoutePolicy)
    ordered_dirs=("aggregationpb" "policypb" "pipelinepb" "metricpb" "rulepb" "transformationpb")
    
    resolve_protos="Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types"
    
    for dir in "${ordered_dirs[@]}"; do
        proto_dir="${GOPATH}/src/${PROTO_SRC}/${dir}"
        if [ -d "$proto_dir" ] && ls $proto_dir/*.proto > /dev/null 2>&1; then
            proto_files=$(ls $proto_dir/*.proto)
            echo "generating from ${proto_files}"

            protoc \
                --gogofaster_out=${resolve_protos},plugins=grpc:${GOPATH}/src \
                -I${GOPATH}/src -I${GOPATH}/src/github.com/m3db/m3/vendor \
                ${proto_files}
        fi
    done
else
    echo "Native protoc not found, using Docker with platform auto-detection"
    # Include portable readlink
    source $(dirname $0)/realpath.sh
    
    PROTOC_IMAGE_VERSION=${PROTOC_IMAGE_VERSION:-"znly/protoc:0.2.0"}
    PLATFORM=$(detect_platform)
    
    echo "Detected platform: $PLATFORM"
    
    # Ensure docker is running
    docker run --platform $PLATFORM --rm hello-world >/dev/null
    
    UID_FLAGS="-u $(id -u)"
    if [[ -n "$BUILDKITE" ]]; then
        UID_FLAGS="-u root"
    fi

    # Define compilation order to handle dependencies  
    # policypb must be compiled before metricpb (metricpb imports policypb.RoutePolicy)
    ordered_dirs=("aggregationpb" "policypb" "pipelinepb" "metricpb" "rulepb" "transformationpb")
    
    # need the additional m3db_path mount in docker because it's a symlink on the CI.
    m3db_path=$(realpath $GOPATH/src/github.com/m3db/m3)
    resolve_protos="Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types"

    # Use platform-specific Docker execution to avoid Rosetta issues
    if [[ "$PLATFORM" == "linux/arm64" ]]; then
        # For ARM64, try to use a multi-arch image or fall back to native
        echo "Warning: Using x86_64 Docker on ARM64 may cause issues. Consider installing native protoc."
        PLATFORM="linux/amd64"  # Fall back to x86_64 with Rosetta risk
    fi

    for dir in "${ordered_dirs[@]}"; do
        proto_dir="${GOPATH}/src/${PROTO_SRC}/${dir}"
        if [ -d "$proto_dir" ] && ls $proto_dir/*.proto > /dev/null 2>&1; then
            proto_files=$(ls $proto_dir/*.proto | sed -e "s@${GOPATH}@@g")
            echo "generating from ${proto_files}"
            
            docker run --platform $PLATFORM --rm -w /src -v $GOPATH/src:/src -v ${m3db_path}:/src/github.com/m3db/m3 \
            $UID_FLAGS $PROTOC_IMAGE_VERSION \
                 --gogofaster_out=${resolve_protos},plugins=grpc:/src \
                 -I/src -I/src/github.com/m3db/m3/vendor ${proto_files}
        fi
    done
fi
