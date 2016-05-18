// Package listmap implements an ordered doubly linked list map.
package listmap

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

var (
	ErrKeyNotFound       = errors.New("listmap: key not found")
	ErrKeyPresent        = errors.New("listmap: key already present")
	ErrFileTruncateError = errors.New("listmap: file truncate error")
	ErrUnknown           = errors.New("listmap: unknown error")
)

const (
	rootLength   = unsafe.Sizeof(root{})
	recordLength = unsafe.Sizeof(record{})

	constTruncateResize = 1 << 16
)

// Listmap represents an ordered doubly linked list map.
type Listmap struct {
	file     *os.File
	fileSize int64
	lock     *sync.Mutex
	root     *root
	mapped   []byte
}

type root struct {
	first        uint64
	last         uint64
	lastInserted uint64
}

type record struct {
	prev    uint64
	next    uint64
	keylen  uint16
	vallen  uint16
	removed bool
}

// NewListmap returns a pointer to an initialized list backed by file
// or nil in the case of an error. file will be truncated.
func NewListmap(file string) (*Listmap, error) {
	f, err := os.Create(file)
	if err != nil {
		return nil, err
	}

	f.Truncate(int64(rootLength))
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	sl, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		f.Close()
		return nil, err
	}

	l := &Listmap{
		file:     f,
		lock:     &sync.Mutex{},
		mapped:   sl,
		fileSize: stat.Size(),
	}

	l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	return l, nil
}

// OpenListmap returns a pointer to an existing Listmap
// backed by file or nil in the case of an error.
func OpenListmap(file string) (*Listmap, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	sl, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		f.Close()
		return nil, err
	}

	l := &Listmap{
		file:     f,
		lock:     &sync.Mutex{},
		mapped:   sl,
		fileSize: stat.Size(),
	}

	l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	return l, nil
}

// Close closes an initialized Listmap.
func (l *Listmap) Close() {
	syscall.Munmap(l.mapped)
	l.file.Close()
}

// Destroy closes an initialized Listmap and
// removes its associated file.
func (l *Listmap) Destroy() {
	syscall.Munmap(l.mapped)
	l.file.Close()
	os.Remove(l.file.Name())
}

// Set writes a key-value pair to a Listmap. Records are
// kept in lexicographical order.
func (l *Listmap) Set(key, value []byte) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	if int64(l.root.lastInserted)+constTruncateResize > int64(len(l.mapped)) {
		syscall.Munmap(l.mapped)
		err := l.file.Truncate(l.fileSize + constTruncateResize)
		if err != nil {
			l.mapped, _ = syscall.Mmap(int(l.file.Fd()), 0, int(l.fileSize),
				syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
			l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
			return ErrFileTruncateError
		}
		l.mapped, _ = syscall.Mmap(int(l.file.Fd()), 0, int(l.fileSize+constTruncateResize),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		l.root = (*root)(unsafe.Pointer(&l.mapped[0]))

		l.fileSize += constTruncateResize
	}

	// First record
	if l.root.lastInserted == 0 {
		r := (*record)(unsafe.Pointer(&l.mapped[rootLength]))
		r.keylen = uint16(len(key))
		r.vallen = uint16(len(value))
		copy(l.mapped[rootLength+recordLength:], append(key, value...))

		l.root.first = uint64(rootLength)
		l.root.last = uint64(rootLength)
		l.root.lastInserted = uint64(rootLength)
		return nil
	}

	cursor := l.NewCursor().seek(int(l.root.lastInserted))

	// New records always go to the end
	currentIndex := int(l.root.lastInserted) + int(recordLength) + int(cursor.r.keylen+cursor.r.vallen)
	l.root.lastInserted = uint64(currentIndex)
	r := (*record)(unsafe.Pointer(&l.mapped[currentIndex]))
	r.keylen = uint16(len(key))
	r.vallen = uint16(len(value))
	copy(l.mapped[currentIndex+int(recordLength):], append(key, value...))

	// Special case: insert at end
	cursor = cursor.seek(int(l.root.last))
	lastKey := cursor.Key()
	if cmp := bytes.Compare(lastKey, key); cmp < 0 || (cmp == 0 && cursor.r.removed) {
		cursor.r.next = uint64(currentIndex)
		r.prev = l.root.last
		l.root.last = cursor.r.next
		return nil
	}

	// Special case: insert at beginning
	cursor = cursor.seek(int(l.root.first))
	firstKey := cursor.Key()
	if cmp := bytes.Compare(firstKey, key); cmp > 0 || (cmp == 0 && cursor.r.removed) {
		cursor.r.prev = uint64(currentIndex)
		r.next = l.root.first
		l.root.first = cursor.r.prev
		return nil
	}

	// find last less than
	cursor = cursor.seek(int(l.root.last))

	for cursor != nil {
		if bytes.Compare(cursor.Key(), key) == 0 &&
			!cursor.r.removed {
			return ErrKeyPresent
		}

		if bytes.Compare(cursor.Key(), key) < 0 {
			previousRecord := cursor.r
			previousRecordIndex := cursor.index
			nextRecord := cursor.Next().r
			nextRecordIndex := cursor.index

			r.next = uint64(nextRecordIndex)
			r.prev = uint64(previousRecordIndex)

			previousRecord.next = uint64(currentIndex)
			nextRecord.prev = uint64(currentIndex)

			return nil
		} else {
			cursor = cursor.Prev()
		}
	}

	return ErrUnknown
}

// Get returns the value in the Listmap associated with key.
func (l *Listmap) Get(key []byte) ([]byte, error) {
	for c := l.NewCursor(); c != nil; c = c.Next() {
		cKey := c.Key()
		if bytes.Compare(cKey, key) > 0 {
			return nil, ErrKeyNotFound
		}

		if bytes.Compare(cKey, key) == 0 {
			if !c.r.removed {
				return c.Value(), nil
			}
		}
	}
	return nil, ErrKeyNotFound
}

// Remove marks a key as removed
func (l *Listmap) Remove(key []byte) {
	for c := l.NewCursor(); c != nil; c = c.Next() {
		cKey := c.Key()
		if bytes.Compare(cKey, key) > 0 {
			return
		}

		if bytes.Compare(cKey, key) == 0 {
			c.r.removed = true
		}
	}
}

// Size returns the current file size of the Listmap.
// Note: this is the raw file size, not the amount
// of data stored in the Listmap.
func (l *Listmap) Size() int {
	return int(l.fileSize)
}
