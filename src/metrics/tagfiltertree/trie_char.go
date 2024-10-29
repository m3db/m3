package tagfiltertree

type trieChar struct {
	ch byte
}

// SetEnd sets the end flag of the character.
func (c *trieChar) SetEnd() {
	c.ch |= 0x01
}

// IsEnd returns true if the character is the end of a pattern.
func (c *trieChar) IsEnd() bool {
	return c.ch&0x01 == 0x01
}

// newTrieChar creates a new trie character.
func newTrieChar(ch byte) trieChar {
	return trieChar{
		ch: ch << 1,
	}
}

// Char returns the character.
func (c *trieChar) Char() byte {
	return c.ch >> 1
}
