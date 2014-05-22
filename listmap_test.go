package listmap

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const N = 2048

func assertOrder(l *Listmap) bool {
	prev := []byte{}
	for c := l.NewCursor(); c != nil; c = c.Next() {
		if bytes.Compare(c.Key(), prev) < 0 {
			return false
		}
	}

	return true
}

func Test1(t *testing.T) {
	l := NewListmap("test.list")

	l.Set([]byte("1"), []byte("bar"))
	l.Set([]byte("2"), []byte("foobar"))
	l.Set([]byte("3"), []byte("barbaz"))
	l.Set([]byte("4"), []byte("b"))
	l.Set([]byte("45"), []byte("foo"))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func Test2(t *testing.T) {
	l := NewListmap("test.list2")

	l.Set([]byte("a"), []byte("AAAAA"))
	l.Set([]byte("c"), []byte("CCCCC"))
	l.Set([]byte("b"), []byte("BBBBB"))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func Test4(t *testing.T) {
	l := NewListmap("test.list4")

	l.Set([]byte("1"), []byte("AAAAA"))
	l.Set([]byte("3"), []byte("CCCCC"))
	l.Set([]byte("2"), []byte("BBBBB"))
	l.Set([]byte("0"), []byte("00000"))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestSequentialShort(t *testing.T) {
	l := NewListmap("test.sequential_short")

	start := time.Now()
	for i := 0; i < N; i++ {
		l.Set([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprint(i)))
	}
	t.Log("Time to insert", N, "sequential integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Close()
}

func TestSequentialLong(t *testing.T) {
	l := NewListmap("test.sequential_long")

	start := time.Now()
	for i := 0; i < N*8; i++ {
		l.Set([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprint(i)))
	}
	t.Log("Time to insert", N*8, "sequential integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRead(t *testing.T) {
	l := OpenListmap("test.sequential_short")
	if l == nil {
		t.Error("Couldn't open list")
	}

	if val, err := l.Get([]byte("000000005")); err != nil || bytes.Compare(val, []byte("5")) != 0 {
		if err != nil {
			t.Error(err)
		} else {
			t.Errorf("expected value %v, got %v", []byte("5"), val)
		}
	}

	if val, err := l.Get([]byte("000000013")); err != nil || bytes.Compare(val, []byte("13")) != 0 {
		if err != nil {
			t.Error(err)
		} else {
			t.Errorf("expected value %v, got %v", []byte("13"), val)
		}
	}

	if val, err := l.Get([]byte("5")); err == nil {
		t.Errorf("expected error `%v', got %v with value `%v'", ErrKeyNotFound,
			nil, val)
	}

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRandomShort(t *testing.T) {
	l := NewListmap("test.random_short")

	start := time.Now()
	for i := 0; i < N; i++ {
		l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i)))
	}
	t.Log("Time to insert", N, "random integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRandomLong(t *testing.T) {
	l := NewListmap("test.random_long")

	start := time.Now()
	for i := 0; i < N*4; i++ {
		l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i)))
	}
	t.Log("Time to insert", N*4, "random integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func BenchmarkSequentialWrites(b *testing.B) {
	l := NewListmap("benchmark.sequential")

	for i := 0; i < b.N; i++ {
		l.Set([]byte(fmt.Sprintf("%020d", i)), []byte(fmt.Sprint(i)))
	}

	l.Destroy()
}
