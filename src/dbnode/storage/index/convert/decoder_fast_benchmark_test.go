package convert

import (
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

type encodedTagsWithTagName struct {
	encodedTags, tagName []byte
}

func BenchmarkTagValueFromEncodedTagsFast(b *testing.B) {
	testData, err := prepareData(b)
	require.NoError(b, err)

	b.ResetTimer()
	for i := range testData {
		_, _, err := serialize.TagValueFromEncodedTagsFast(testData[i].encodedTags,
			testData[i].tagName)
		require.NoError(b, err)
	}
}

func BenchmarkTagValueFromEncodedTagsFast2(b *testing.B) {
	testData, err := prepareData(b)
	require.NoError(b, err)

	b.ResetTimer()
	for i := range testData {
		_, _, err := serialize.TagValueFromEncodedTagsFast2(testData[i].encodedTags,
			testData[i].tagName)
		require.NoError(b, err)
	}
}

func prepareData(b *testing.B) ([]encodedTagsWithTagName, error) {
	data, err := prepareIDAndEncodedTags(b)
	if err != nil {
		return nil, err
	}

	decoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		pool.NewObjectPoolOptions(),
	)
	decoderPool.Init()
	decoder := decoderPool.Get()
	defer decoder.Close()

	var tagNames [][]byte
	decoder.Reset(checked.NewBytes(data[0].encodedTags, nil))
	for decoder.Next() {
		tagNames = append(tagNames, clone(decoder.Current().Name.Bytes()))
	}
	tagNames = append(tagNames, []byte("not_exist"))

	var (
		result = make([]encodedTagsWithTagName, 0, b.N)
		rnd    = rand.New(rand.NewSource(42))
	)

	for i := 0; i < b.N; i++ {
		result = append(result, encodedTagsWithTagName{
			encodedTags: data[i].encodedTags,
			tagName:     tagNames[rnd.Intn(len(tagNames))],
		})
	}

	return result, nil
}
