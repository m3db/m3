
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"path"
	"reflect"

	"github.com/golang/mock/mockgen/model"

	pkg_ "github.com/m3db/m3/src/dbnode/persist/fs"
)

var output = flag.String("output", "", "The output file name, or empty to use stdout.")

func main() {
	flag.Parse()

	its := []struct{
		sym string
		typ reflect.Type
	}{
		
		{ "DataFileSetWriter", reflect.TypeOf((*pkg_.DataFileSetWriter)(nil)).Elem()},
		
		{ "DataFileSetReader", reflect.TypeOf((*pkg_.DataFileSetReader)(nil)).Elem()},
		
		{ "DataFileSetSeeker", reflect.TypeOf((*pkg_.DataFileSetSeeker)(nil)).Elem()},
		
		{ "IndexFileSetWriter", reflect.TypeOf((*pkg_.IndexFileSetWriter)(nil)).Elem()},
		
		{ "IndexFileSetReader", reflect.TypeOf((*pkg_.IndexFileSetReader)(nil)).Elem()},
		
		{ "IndexSegmentFileSetWriter", reflect.TypeOf((*pkg_.IndexSegmentFileSetWriter)(nil)).Elem()},
		
		{ "IndexSegmentFileSet", reflect.TypeOf((*pkg_.IndexSegmentFileSet)(nil)).Elem()},
		
		{ "IndexSegmentFile", reflect.TypeOf((*pkg_.IndexSegmentFile)(nil)).Elem()},
		
		{ "SnapshotMetadataFileWriter", reflect.TypeOf((*pkg_.SnapshotMetadataFileWriter)(nil)).Elem()},
		
		{ "DataFileSetSeekerManager", reflect.TypeOf((*pkg_.DataFileSetSeekerManager)(nil)).Elem()},
		
		{ "ConcurrentDataFileSetSeeker", reflect.TypeOf((*pkg_.ConcurrentDataFileSetSeeker)(nil)).Elem()},
		
		{ "MergeWith", reflect.TypeOf((*pkg_.MergeWith)(nil)).Elem()},
		
	}
	pkg := &model.Package{
		// NOTE: This behaves contrary to documented behaviour if the
		// package name is not the final component of the import path.
		// The reflect package doesn't expose the package name, though.
		Name: path.Base("github.com/m3db/m3/src/dbnode/persist/fs"),
	}

	for _, it := range its {
		intf, err := model.InterfaceFromInterfaceType(it.typ)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Reflection: %v\n", err)
			os.Exit(1)
		}
		intf.Name = it.sym
		pkg.Interfaces = append(pkg.Interfaces, intf)
	}

	outfile := os.Stdout
	if len(*output) != 0 {
		var err error
		outfile, err = os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open output file %q", *output)
		}
		defer func() {
			if err := outfile.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "failed to close output file %q", *output)
				os.Exit(1)
			}
		}()
	}

	if err := gob.NewEncoder(outfile).Encode(pkg); err != nil {
		fmt.Fprintf(os.Stderr, "gob encode: %v\n", err)
		os.Exit(1)
	}
}
