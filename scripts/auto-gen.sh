#!/bin/bash
source "$(dirname $0)/auto-gen-helpers.sh"
set -e

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "usage: auto-gen.sh output_directory file_generation_rules_directory"
    exit 1
fi

if [[ "$2" = *"generated/mocks"* ]]; then
    remove_matching_files $1 "*_mock.go"
    remove_matching_files $1 "*_mock_test.go"
elif [[ "$2" = *"generated/generics"* ]]; then
    remove_matching_files $1 "*_gen.go"
elif [[ "$2" = *"generated/proto"* ]]; then
    remove_matching_files $1 "*.pb.go"
else
    autogen_clear $1
fi

go generate $PACKAGE/$2

if [[ "$2" = *"generated/mocks"* ]]; then
    gen_cleanup "*_mock.go"
    gen_cleanup "*_mock_test.go"
elif [[ "$2" = *"generated/generics"* ]]; then
    gen_cleanup "*_gen.go"
else
    autogen_cleanup $1
fi
