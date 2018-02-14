package mem

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"
)

var (
	benchRegexFetchName   = []byte("__name__")
	benchRegexFetchFilter = []byte("node_netstat_Tcp_.*")
	benchRegexFetchOpts   = termFetchOptions{true}
)

func BenchmarkTermsDictionary(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(docs []doc.Document, b *testing.B)
	}{
		{
			name: "benchmark inserting documents into simple terms dictionary",
			fn:   benchmarkInsertSimpleTermsDictionary,
		},
		{
			name: "benchmark inserting documents into trigram terms dictionary",
			fn:   benchmarkInsertTrigramTermsDictionary,
		},
		{
			name: "benchmark fetching documents from simple terms dictionary",
			fn:   benchmarkFetchSimpleTermsDictionary,
		},
		{
			name: "benchmark fetching documents from trigram terms dictionary",
			fn:   benchmarkFetchTrigramTermsDictionary,
		},
		{
			name: "benchmark regex fetch for simple terms dictionary",
			fn:   benchmarkFetchRegexSimpleTermsDictionary,
		},
		{
			name: "benchmark regex fetch for trigram terms dictionary",
			fn:   benchmarkFetchRegexTrigramTermsDictionary,
		},
	}

	docs, err := readDocuments("../../../testdata/node_exporter.json", 2000)
	if err != nil {
		b.Fatalf("unable to read documents for benchmarks: %v", err)
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			bm.fn(docs, b)
		})
	}
}

func benchmarkInsertSimpleTermsDictionary(docs []doc.Document, b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dict := newSimpleTermsDictionary(NewOptions())
		b.StartTimer()

		for i, d := range docs {
			for _, f := range d.Fields {
				dict.Insert(f, segment.DocID(i))
			}
		}
	}
}

func benchmarkInsertTrigramTermsDictionary(docs []doc.Document, b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dict := newTrigramTermsDictionary(NewOptions())
		b.StartTimer()

		for i, d := range docs {
			for _, f := range d.Fields {
				dict.Insert(f, segment.DocID(i))
			}
		}
	}
}

func benchmarkFetchSimpleTermsDictionary(docs []doc.Document, b *testing.B) {
	dict := newSimpleTermsDictionary(NewOptions())
	for i, d := range docs {
		for _, f := range d.Fields {
			dict.Insert(f, segment.DocID(i))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, d := range docs {
			for _, f := range d.Fields {
				dict.Fetch(f.Name, f.Value, termFetchOptions{false})
			}
		}
	}
}

func benchmarkFetchTrigramTermsDictionary(docs []doc.Document, b *testing.B) {
	dict := newTrigramTermsDictionary(NewOptions())
	for i, d := range docs {
		for _, f := range d.Fields {
			dict.Insert(f, segment.DocID(i))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, d := range docs {
			for _, f := range d.Fields {
				// The trigram terms dictionary can return false postives so we may want to
				// consider verifying the results returned are matches to provide a more
				// fair comparison with the simple terms dictionary.
				dict.Fetch(f.Name, f.Value, termFetchOptions{false})
			}
		}
	}
}

func benchmarkFetchRegexSimpleTermsDictionary(docs []doc.Document, b *testing.B) {
	dict := newSimpleTermsDictionary(NewOptions())
	for i, d := range docs {
		for _, f := range d.Fields {
			dict.Insert(f, segment.DocID(i))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dict.Fetch(benchRegexFetchName, benchRegexFetchFilter, benchRegexFetchOpts)
	}
}

func benchmarkFetchRegexTrigramTermsDictionary(docs []doc.Document, b *testing.B) {
	dict := newTrigramTermsDictionary(NewOptions())
	for i, d := range docs {
		for _, f := range d.Fields {
			dict.Insert(f, segment.DocID(i))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// The trigram terms dictionary can return false postives so we may want to
		// consider verifying the results returned are matches to provide a more
		// fair comparison with the simple terms dictionary.
		dict.Fetch(benchRegexFetchName, benchRegexFetchFilter, benchRegexFetchOpts)
	}
}

func readDocuments(fn string, n int) ([]doc.Document, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var (
		docs    []doc.Document
		scanner = bufio.NewScanner(f)
	)
	for scanner.Scan() && len(docs) < n {
		var fieldsMap map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &fieldsMap); err != nil {
			return nil, err
		}

		fields := make([]doc.Field, 0, len(fieldsMap))
		for k, v := range fieldsMap {
			fields = append(fields, doc.Field{
				Name:  []byte(k),
				Value: doc.Value(v),
			})
		}
		docs = append(docs, doc.Document{
			Fields: fields,
		})
	}

	if len(docs) != n {
		return nil, fmt.Errorf("requested %d metrics but found %d", n, len(docs))
	}

	return docs, nil
}
