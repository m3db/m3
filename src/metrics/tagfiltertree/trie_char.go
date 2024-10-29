package tagfiltertree

type trieChar struct {
	ch byte
}

func (c *trieChar) SetEnd() {
	c.ch |= 0x01
}

func (c *trieChar) IsEnd() bool {
	return c.ch&0x01 == 0x01
}

func NewTrieChar(ch byte) trieChar {
	return trieChar{
		ch: ch << 1,
	}
}

func (c *trieChar) Char() byte {
	return c.ch >> 1
}
