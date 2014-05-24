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

	constTruncateResize = 1 << 12
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
	first        uint32
	last         uint32
	lastInserted uint32
}

type record struct {
	prev    uint32
	next    uint32
	keylen  uint16
	vallen  uint16
	removed bool
}

// NewListmap returns a pointer to an initialized list backed by file
// or nil in the case of an error. file will be truncated.
func NewListmap(file string) *Listmap {
	f, err := os.Create(file)
	if err != nil {
		return nil
	}

	f.Truncate(int64(rootLength))
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil
	}

	sl, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		f.Close()
		return nil
	}

	l := &Listmap{
		file:     f,
		lock:     &sync.Mutex{},
		mapped:   sl,
		fileSize: stat.Size(),
	}

	l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	return l
}

// OpenListmap returns a pointer to an existing Listmap
// backed by file or nil in the case of an error.
func OpenListmap(file string) *Listmap {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil
	}

	sl, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		f.Close()
		return nil
	}

	l := &Listmap{
		file:     f,
		lock:     &sync.Mutex{},
		mapped:   sl,
		fileSize: stat.Size(),
	}

	l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	return l
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

		l.root.first = uint32(rootLength)
		l.root.last = uint32(rootLength)
		l.root.lastInserted = uint32(rootLength)
		l.lock.Unlock()
		return nil
	}

	cursor := l.NewCursor().seek(int(l.root.lastInserted))

	// New records always go to the end
	currentIndex := int(l.root.lastInserted) + int(recordLength) + int(cursor.r.keylen+cursor.r.vallen)
	l.root.lastInserted = uint32(currentIndex)
	r := (*record)(unsafe.Pointer(&l.mapped[currentIndex]))
	r.keylen = uint16(len(key))
	r.vallen = uint16(len(value))
	copy(l.mapped[currentIndex+int(recordLength):], append(key, value...))

	l.lock.Unlock()

	cursor = cursor.seek(int(l.root.last))

	lastKey := cursor.Key()

	// Sequential insert
	if cmp := bytes.Compare(lastKey, key); cmp < 0 || (cmp == 0 && cursor.r.removed) {
		cursor.r.next = uint32(currentIndex)
		r.prev = l.root.last
		l.root.last = cursor.r.next
		return nil
	} else {
		// find first greater than
		cursor = cursor.seek(int(l.root.first))

		for cursor != nil {
			if bytes.Compare(cursor.Key(), key) == 0 &&
				!cursor.r.removed {
				return ErrKeyPresent
			}

			if bytes.Compare(cursor.Key(), key) > 0 {
				if cursor.index == int(l.root.first) {
					// inserting before first
					cursor.r.prev = uint32(currentIndex)
					r.next = uint32(cursor.index)
					l.root.first = uint32(currentIndex)
					return nil
				} else {
					nextRecord := cursor.r
					nextRecordIndex := cursor.index
					previousRecord := cursor.Prev().r
					previousRecordIndex := cursor.index

					r.next = uint32(nextRecordIndex)
					r.prev = uint32(previousRecordIndex)

					previousRecord.next = uint32(currentIndex)
					nextRecord.prev = uint32(currentIndex)

					return nil
				}
			} else {
				cursor = cursor.Next()
			}
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
