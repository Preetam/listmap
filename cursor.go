package listmap

import (
	"unsafe"
)

// Cursor represents a cursor in the map.
type Cursor struct {
	l     *Listmap
	index int
	r     *record
}

// NewCursor returns a pointer to a cursor
// positioned at the first element of the Listmap.
func (l *Listmap) NewCursor() *Cursor {
	return &Cursor{
		l:     l,
		index: int(l.root.first),
		r:     (*record)(unsafe.Pointer(&l.mapped[int(l.root.first)])),
	}
}

// seek is not exported because indices are not accessible to the user.
func (c *Cursor) seek(i int) *Cursor {
	c.index = i
	c.r = (*record)(unsafe.Pointer(&c.l.mapped[i]))
	return c
}

// Next moves the cursor to the next element in
// the Listmap and returns a pointer to itself or
// nil if the end of the list is reached. This
// modifies the original cursor.
func (c *Cursor) Next() *Cursor {
	next := int(c.r.next)
	if next == 0 {
		return nil
	}

	c.index = next
	c.r = (*record)(unsafe.Pointer(&c.l.mapped[next]))

	return c
}

// Prev moves the cursor to the previous element in
// the Listmap and returns a pointer to itself or
// nil when moved behind the first element. This
// modifies the original cursor.
func (c *Cursor) Prev() *Cursor {
	prev := int(c.r.prev)
	if prev == 0 {
		return nil
	}

	c.index = prev
	c.r = (*record)(unsafe.Pointer(&c.l.mapped[prev]))

	return c
}

// Key returns the key of the element at the current
// location of the cursor. The returned slice is a subslice
// of the memory-mapped file, so modifications may lead
// to corruption of the list.
func (c *Cursor) Key() []byte {
	start := c.index + int(recordLength)
	end := start + int(c.r.keylen)
	return c.l.mapped[start:end]
}

// Value returns the value of the element at the current
// location of the cursor. The returned slice is a subslice
// of the memory-mapped file, so modifications may lead
// to corruption of the list.
func (c *Cursor) Value() []byte {
	start := c.index + int(recordLength) + int(c.r.keylen)
	end := start + int(c.r.vallen)
	return c.l.mapped[start:end]
}
