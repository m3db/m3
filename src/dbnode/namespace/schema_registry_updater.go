// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package namespace

import (
	"fmt"

	xerrors "github.com/m3db/m3/src/x/errors"

	"go.uber.org/zap"
)

// UpdateSchemaRegistry updates schema registry with namespace updates.
func UpdateSchemaRegistry(newNamespaces Map, schemaReg SchemaRegistry, log *zap.Logger) error {
	schemaUpdates := newNamespaces.Metadatas()
	merr := xerrors.NewMultiError()
	for _, metadata := range schemaUpdates {
		curSchemaID := "none"
		curSchema, err := schemaReg.GetLatestSchema(metadata.ID())
		if curSchema != nil {
			curSchemaID = curSchema.DeployId()
			if len(curSchemaID) == 0 {
				log.Warn("can not update namespace schema with empty deploy ID", zap.Stringer("namespace", metadata.ID()),
					zap.String("currentSchemaID", curSchemaID))
				merr = merr.Add(fmt.Errorf("can not update namespace(%v) schema with empty deploy ID", metadata.ID().String()))
				continue
			}
		}
		// Log schema update.
		latestSchema, found := metadata.Options().SchemaHistory().GetLatest()
		if !found {
			log.Warn("can not update namespace schema to empty", zap.Stringer("namespace", metadata.ID()),
				zap.String("currentSchema", curSchemaID))
			merr = merr.Add(fmt.Errorf("can not update namespace(%v) schema to empty", metadata.ID().String()))
			continue
		}
		log.Info("updating database namespace schema", zap.Stringer("namespace", metadata.ID()),
			zap.String("currentSchema", curSchemaID), zap.String("latestSchema", latestSchema.DeployId()))

		err = schemaReg.SetSchemaHistory(metadata.ID(), metadata.Options().SchemaHistory())
		if err != nil {
			log.Warn("failed to update latest schema for namespace",
				zap.Stringer("namespace", metadata.ID()),
				zap.Error(err))
			merr = merr.Add(fmt.Errorf("failed to update latest schema for namespace %v, error: %v",
				metadata.ID().String(), err))
		}
	}
	if merr.Empty() {
		return nil
	}
	return merr
}

