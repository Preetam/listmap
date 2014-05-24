package listmap

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const N = 1 << 11

func assertOrder(l *Listmap) bool {
	prev := []byte{}
	for c := l.NewCursor(); c != nil; c = c.Next() {
		if bytes.Compare(c.Key(), prev) < 0 {
			return false
		}
	}

	return true
}

func assertZeroMissingKeys(l *Listmap) bool {
	i := 0
	for c := l.NewCursor(); c != nil; c = c.Next() {
		expectedKey := []byte(fmt.Sprintf("%020d", i))
		if bytes.Compare(c.Key(), expectedKey) != 0 {
			return false
		}

		i++
	}

	return true
}

func checkError(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func Test1(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.1")

	checkError(l.Set([]byte("1"), []byte("bar")), t)
	checkError(l.Set([]byte("2"), []byte("foobar")), t)
	checkError(l.Set([]byte("3"), []byte("barbaz")), t)
	checkError(l.Set([]byte("4"), []byte("b")), t)
	checkError(l.Set([]byte("45"), []byte("foo")), t)

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func Test2(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.2")

	checkError(l.Set([]byte("a"), []byte("AAAAA")), t)
	checkError(l.Set([]byte("c"), []byte("CCCCC")), t)
	checkError(l.Set([]byte("b"), []byte("BBBBB")), t)

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func Test3(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.3")

	checkError(l.Set([]byte("1"), []byte("AAAAA")), t)
	checkError(l.Set([]byte("3"), []byte("CCCCC")), t)
	checkError(l.Set([]byte("2"), []byte("BBBBB")), t)
	checkError(l.Set([]byte("0"), []byte("00000")), t)

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRemove(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.remove")

	checkError(l.Set([]byte("foo"), []byte("bar")), t)
	val, err := l.Get([]byte("foo"))
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(val, []byte("bar")) != 0 {
		t.Errorf("expected value to be %v, got %v", []byte("bar"), val)
	}

	l.Remove([]byte("foo"))

	val, err = l.Get([]byte("foo"))
	if err != ErrKeyNotFound {
		t.Errorf("expected error `%v', got %v", ErrKeyNotFound, err)
	}

	if bytes.Compare(val, nil) != 0 {
		t.Errorf("expected value to be %v, got %v", nil, val)
	}

	checkError(l.Set([]byte("foo"), []byte("baz")), t)
	val, err = l.Get([]byte("foo"))
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(val, []byte("baz")) != 0 {
		t.Errorf("expected value to be %v, got %v", []byte("baz"), val)
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
	t.Parallel()
	l := NewListmap("test.sequential_long")

	start := time.Now()
	for i := 0; i < N*8; i++ {
		checkError(l.Set([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprint(i))), t)
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
	t.Parallel()
	l := NewListmap("test.random_short")

	start := time.Now()
	for i := 0; i < N; i++ {
		checkError(l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i))), t)
	}
	t.Log("Time to insert", N, "random integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestRandomLong(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.random_long")

	start := time.Now()
	for i := 0; i < N*8; i++ {
		checkError(l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i))), t)
	}
	t.Log("Time to insert", N*8, "random integers:", time.Now().Sub(start))

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	l.Destroy()
}

func TestConcurrentSequential(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.concurrent_sequential")
	var wg sync.WaitGroup

	run := func(l *Listmap, n int) {
		defer wg.Done()
		for i := 0; i < N*4; i++ {
			if i%10 == n {
				checkError(l.Set([]byte(fmt.Sprintf("%020d", i)), []byte(fmt.Sprint(i))), t)
			}
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go run(l, i)
	}

	wg.Wait()

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	if !assertZeroMissingKeys(l) {
		t.Error("there are missing keys")
	}

	l.Destroy()
}

func TestConcurrentSequential2(t *testing.T) {
	t.Parallel()
	rand.Seed(time.Now().Unix())
	l := NewListmap("test.concurrent_sequential_2")
	var wg sync.WaitGroup

	run := func(l *Listmap, n int) {
		defer wg.Done()
		for i := 0; i < N*4; i++ {
			if rand.Float32() < 0.85 {
				l.Set([]byte(fmt.Sprintf("%020d", i)), []byte(fmt.Sprint(i)))
			}
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go run(l, i)
	}

	wg.Wait()

	if !assertOrder(l) {
		t.Error("keys were not in order")
	}

	if !assertZeroMissingKeys(l) {
		t.Error("there are missing keys")
	}

	l.Destroy()
}

func TestConcurrentRandom(t *testing.T) {
	t.Parallel()
	l := NewListmap("test.concurrent_random")
	var wg sync.WaitGroup

	run := func(l *Listmap, n int) {
		defer wg.Done()
		for i := 0; i < N*4; i++ {
			if i%10 == n {
				checkError(l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i))), t)
			}
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go run(l, i)
	}

	wg.Wait()

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

func BenchmarkRandomWrites(b *testing.B) {
	l := NewListmap("benchmark.sequential")

	for i := 0; i < b.N; i++ {
		l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i)))
	}

	l.Destroy()
}

func BenchmarkSequentialWritesWithVerification(b *testing.B) {
	l := NewListmap("benchmark.sequential")

	for i := 0; i < b.N; i++ {
		l.Set([]byte(fmt.Sprintf("%020d", i)), []byte(fmt.Sprint(i)))
	}

	if !assertOrder(l) {
		b.Error("keys were not in order")
	}

	l.Destroy()
}

func BenchmarkRandomWritesWithVerification(b *testing.B) {
	l := NewListmap("benchmark.sequential")

	for i := 0; i < b.N; i++ {
		l.Set([]byte(fmt.Sprint(rand.Int())), []byte(fmt.Sprint(i)))
	}

	if !assertOrder(l) {
		b.Error("keys were not in order")
	}

	l.Destroy()
}
