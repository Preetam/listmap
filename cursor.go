package list

import (
	"unsafe"
)

type Cursor struct {
	l     *List
	index int
	r     *record
}

func (l *List) NewCursor() *Cursor {
	return &Cursor{
		l:     l,
		index: int(l.root.first),
		r:     (*record)(unsafe.Pointer(&l.mapped[int(l.root.first)])),
	}
}

func (c *Cursor) seek(i int) *Cursor {
	c.index = i
	c.r = (*record)(unsafe.Pointer(&c.l.mapped[i]))
	return c
}

func (c *Cursor) Next() *Cursor {
	next := int(c.r.next)
	if next == 0 {
		return nil
	}

	c.index = next
	c.r = (*record)(unsafe.Pointer(&c.l.mapped[next]))

	return c
}

func (c *Cursor) Prev() *Cursor {
	prev := int(c.r.prev)
	if prev == 0 {
		return nil
	}

	c.index = prev
	c.r = (*record)(unsafe.Pointer(&c.l.mapped[prev]))

	return c
}

func (c *Cursor) Key() []byte {
	start := c.index + int(recordLength)
	end := start + int(c.r.keylen)
	return c.l.mapped[start:end]
}

func (c *Cursor) Value() []byte {
	start := c.index + int(recordLength) + int(c.r.keylen)
	end := start + int(c.r.vallen)
	return c.l.mapped[start:end]
}
