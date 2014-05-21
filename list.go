package list

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

var (
	ErrKeyNotFound = errors.New("list: key not found")
)

const (
	rootLength   = unsafe.Sizeof(root{})
	recordLength = unsafe.Sizeof(record{})
)

type List struct {
	file   *os.File
	lock   *sync.Mutex
	root   *root
	mapped []byte
}

type root struct {
	first        uint32
	last         uint32
	lastInserted uint32
}

type record struct {
	prev   uint32
	next   uint32
	keylen uint16
	vallen uint16
}

func NewList(file string) *List {
	f, err := os.Create(file)
	if err != nil {
		return nil
	}

	f.Truncate(1 << 4)
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

	l := &List{
		file:   f,
		lock:   &sync.Mutex{},
		mapped: sl,
	}

	l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	return l
}

func OpenList(file string) *List {
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

	l := &List{
		file:   f,
		lock:   &sync.Mutex{},
		mapped: sl,
	}

	l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	return l
}

func (l *List) Close() {
	syscall.Munmap(l.mapped)
	l.file.Close()
}

func (l *List) Destroy() {
	syscall.Munmap(l.mapped)
	l.file.Close()
	os.Remove(l.file.Name())
}

func (l *List) Set(key, value string) {
	l.lock.Lock()

	stat, _ := l.file.Stat()
	if int64(l.root.lastInserted)+1<<23 > int64(len(l.mapped)) {
		syscall.Munmap(l.mapped)
		l.file.Truncate(stat.Size() + 1<<23)
		l.mapped, _ = syscall.Mmap(int(l.file.Fd()), 0, int(stat.Size()+1<<12),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		l.root = (*root)(unsafe.Pointer(&l.mapped[0]))
	}

	// First record
	if l.root.lastInserted == 0 {
		r := (*record)(unsafe.Pointer(&l.mapped[rootLength]))
		r.keylen = uint16(len(key))
		r.vallen = uint16(len(value))
		copy(l.mapped[rootLength+recordLength:], []byte(key+value))

		l.root.first = uint32(rootLength)
		l.root.last = uint32(rootLength)
		l.root.lastInserted = uint32(rootLength)
		l.lock.Unlock()
		return
	}

	cursor := l.NewCursor().seek(int(l.root.lastInserted))

	// New records always go to the end
	currentIndex := int(l.root.lastInserted) + int(recordLength) + int(cursor.r.keylen+cursor.r.vallen)
	l.root.lastInserted = uint32(currentIndex)
	r := (*record)(unsafe.Pointer(&l.mapped[currentIndex]))
	r.keylen = uint16(len(key))
	r.vallen = uint16(len(value))
	copy(l.mapped[currentIndex+int(recordLength):], []byte(key+value))

	l.lock.Unlock()

	cursor = cursor.seek(int(l.root.last))

	lastKey := cursor.Key()

	// Sequential insert
	if lastKey < key {
		cursor.r.next = uint32(currentIndex)
		r.prev = l.root.last
		l.root.last = cursor.r.next
	} else {
		// find first greater than

		cursor = cursor.seek(int(l.root.first))

		for cursor != nil {
			if cursor.Key() > key {
				if cursor.index == int(l.root.first) {
					// inserting before first
					cursor.r.prev = uint32(currentIndex)
					r.next = uint32(cursor.index)
					l.root.first = uint32(currentIndex)
					return
				} else {
					nextRecord := cursor.r
					nextRecordIndex := cursor.index
					previousRecord := cursor.Prev().r
					previousRecordIndex := cursor.index

					r.next = uint32(nextRecordIndex)
					r.prev = uint32(previousRecordIndex)

					previousRecord.next = uint32(currentIndex)
					nextRecord.prev = uint32(currentIndex)

					return
				}
			} else {
				cursor = cursor.Next()
			}
		}
	}
}

func (l *List) Get(key string) (string, error) {
	for c := l.NewCursor(); c != nil; c = c.Next() {
		cKey := c.Key()
		if cKey > key {
			return "", ErrKeyNotFound
		}

		if cKey == key {
			return c.Value(), nil
		}
	}
	return "", ErrKeyNotFound
}
