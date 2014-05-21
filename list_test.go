package list

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const N = 2048

func assertOrder(l *List) bool {
	prev := ""
	for c := l.NewCursor(); c != nil; c = c.Next() {
		if c.Key() < prev {
			return false
		}
	}

	return true
}

func Test1(t *testing.T) {
	l := NewList("test.list")

	l.Set("1", "bar")
	l.Set("2", "foobar")
	l.Set("3", "barbaz")
	l.Set("4", "b")
	l.Set("45", "foo")

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func Test2(t *testing.T) {
	l := NewList("test.list2")

	l.Set("a", "AAAAA")
	l.Set("c", "CCCCC")
	l.Set("b", "BBBBB")

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func Test4(t *testing.T) {
	l := NewList("test.list4")

	l.Set("1", "AAAAA")
	l.Set("3", "CCCCC")
	l.Set("2", "BBBBB")
	l.Set("0", "00000")

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestSequentialShort(t *testing.T) {
	l := NewList("test.sequential_short")

	start := time.Now()
	for i := 0; i < N; i++ {
		l.Set(fmt.Sprintf("%09d", i), fmt.Sprint(i))
	}
	t.Log("Time to insert", N, "sequential integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Close()
}

func TestSequentialLong(t *testing.T) {
	l := NewList("test.sequential_long")

	start := time.Now()
	for i := 0; i < N*8; i++ {
		l.Set(fmt.Sprintf("%09d", i), fmt.Sprint(i))
	}
	t.Log("Time to insert", N*8, "sequential integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRead(t *testing.T) {
	l := OpenList("test.sequential_short")
	if l == nil {
		t.Error("Couldn't open list")
	}

	if val, err := l.Get("000000005"); err != nil || val != "5" {
		if err != nil {
			t.Error(err)
		} else {
			t.Errorf("expected value %v, got %v", "5", val)
		}
	}

	if val, err := l.Get("000000013"); err != nil || val != "13" {
		if err != nil {
			t.Error(err)
		} else {
			t.Errorf("expected value %v, got %v", "13", val)
		}
	}

	if val, err := l.Get("5"); err == nil {
		t.Errorf("expected error `%v', got %v with value `%v'", ErrKeyNotFound,
			nil, val)
	}

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRandomShort(t *testing.T) {
	l := NewList("test.random_short")

	start := time.Now()
	for i := 0; i < N; i++ {
		l.Set(fmt.Sprint(rand.Int()), fmt.Sprint(i))
	}
	t.Log("Time to insert", N, "random integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRandomLong(t *testing.T) {
	l := NewList("test.random_long")

	start := time.Now()
	for i := 0; i < N*4; i++ {
		l.Set(fmt.Sprint(rand.Int()), fmt.Sprint(i))
	}
	t.Log("Time to insert", N*4, "random integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func BenchmarkSequentialWrites(b *testing.B) {
	l := NewList("benchmark.sequential")

	for i := 0; i < b.N; i++ {
		l.Set(fmt.Sprintf("%020d", i), fmt.Sprint(i))
	}

	l.Destroy()
}
