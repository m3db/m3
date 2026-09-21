package aggregator

import "strings"

const (
	_tagPairSplitter     = ","
	_tagPairSplitterByte = ','
	_tagNameSplitter     = "="
)

func ExtractSourceTag(id string, sourceTag string) (string, bool) {
	var (
		sourceTagAssign     = _tagPairSplitter + sourceTag + _tagNameSplitter
		minimumSourceTagLen = len(sourceTagAssign) + 1
	)
	idlen := len(id)
	if idlen < minimumSourceTagLen {
		return "", false
	}

	idx := strings.Index(id, sourceTagAssign)
	if idx < 0 {
		return "", false
	}

	// The front of the ID is now the tag value, along with the remainder of the
	// ID's tags. Find the next tag splitter, if any.
	id = id[idx+len(sourceTagAssign):]
	idx = strings.IndexByte(id, _tagPairSplitterByte)

	switch idx {
	case -1: // delimiter not found; remainder of ID is the service name
		return id, len(id) > 0
	case 0: // id[0] = ',' - i.e., no tag value
		return "", false
	default: // id[:idx] == tag value
		return id[:idx], true
	}
}
