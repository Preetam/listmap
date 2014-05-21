package list

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

var ErrKeyNotFound = errors.New("list: key not found")

const rootLength = unsafe.Sizeof(root{})
const recordLength = unsafe.Sizeof(record{})

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
	fmt.Println(unsafe.Pointer(l.root))
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
	defer l.lock.Unlock()

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
		return
	}

	lastInsertedRecord := l.lastInsertedRecord()

	lastKey := l.getKeyAtRecord(int(l.root.last))

	// New records always go to the end
	currentIndex := int(l.root.lastInserted) + int(recordLength) + int(lastInsertedRecord.keylen+lastInsertedRecord.vallen)
	l.root.lastInserted = uint32(currentIndex)
	r := (*record)(unsafe.Pointer(&l.mapped[currentIndex]))
	r.keylen = uint16(len(key))
	r.vallen = uint16(len(value))
	copy(l.mapped[currentIndex+int(recordLength):], []byte(key+value))

	// Sequential insert
	if lastKey < key {
		lastInsertedRecord.next = uint32(currentIndex)
		r.prev = l.root.last
		l.root.last = lastInsertedRecord.next
	} else {
		// find first greater than

		cur := int(l.root.first)
		for cur != 0 {
			if l.getKeyAtRecord(cur) > key {
				if cur == int(l.root.first) {
					// inserting before first
					firstRecord := l.getRecordAtIndex(int(l.root.first))
					firstRecord.prev = uint32(currentIndex)
					r.next = uint32(cur)
					l.root.first = uint32(currentIndex)
					return
				} else {
					previousRecord := l.getRecordAtIndex(l.prevIndex(cur))
					nextRecord := l.getRecordAtIndex(cur)
					previousRecord.next = uint32(currentIndex)
					r.prev = uint32(nextRecord.prev)
					nextRecord.prev = uint32(currentIndex)
					r.next = uint32(cur)
					return
				}
			} else {
				cur = l.nextIndex(cur)
			}
		}
	}
}

func (l *List) Get(key string) (string, error) {
	return "", ErrKeyNotFound
}

func (l *List) lastInsertedRecord() *record {
	return (*record)(unsafe.Pointer(&l.mapped[int(l.root.lastInserted)]))
}

func (l *List) getRecordAtIndex(i int) *record {
	return (*record)(unsafe.Pointer(&l.mapped[i]))
}

func (l *List) getKeyAtRecord(i int) string {
	r := (*record)(unsafe.Pointer(&l.mapped[i]))
	return string(l.mapped[i+int(recordLength) : i+int(recordLength)+int(r.keylen)])
}

func (l *List) getValueAtRecord(i int) string {
	r := (*record)(unsafe.Pointer(&l.mapped[i]))
	return string(l.mapped[i+int(recordLength)+int(r.keylen) : i+int(recordLength)+int(r.keylen)+int(r.vallen)])
}

func (l *List) nextIndex(i int) int {
	r := (*record)(unsafe.Pointer(&l.mapped[i]))
	return int(r.next)
}

func (l *List) prevIndex(i int) int {
	r := (*record)(unsafe.Pointer(&l.mapped[i]))
	return int(r.prev)
}

func (l *List) printRecordAtIndex(i int) {
	r := (*record)(unsafe.Pointer(&l.mapped[i]))
	fmt.Printf("[prev: %d, next: %d] -- %s => %s\n", r.prev, r.next, l.getKeyAtRecord(i), l.getValueAtRecord(i))
}
