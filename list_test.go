package list

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func iterate(l *List) {
	cur := l.root.first
	fmt.Println("cur:", cur)
	for cur != 0 {
		l.printRecordAtIndex(int(cur))
		cur = uint32(l.nextIndex(int(cur)))
	}
}

func Test1(t *testing.T) {
	l := NewList("test.list")

	t.Log(l.root)

	l.Set("1", "bar")
	l.Set("2", "baz")
	l.Set("3", "baz")
	l.Set("4", "baz")
	l.Set("45", "baz")

	l.Destroy()
}

func Test2(t *testing.T) {
	l := NewList("test.list2")

	t.Log(l.root)

	l.Set("a", "AAAAA")
	l.Set("c", "CCCCC")
	l.Set("b", "BBBBB")

	l.Destroy()
}

func Test3(t *testing.T) {
	l := NewList("test.list3")

	for i := 0; i < 10; i++ {
		n := rand.Intn(1024)
		t.Logf("inserting `%d'\n", n)
		l.Set(fmt.Sprint(n), ".")
	}

	l.Destroy()
}

func Test4(t *testing.T) {
	l := NewList("test.list4")

	t.Log(l.root)

	l.Set("1", "AAAAA")
	l.Set("3", "CCCCC")
	l.Set("2", "BBBBB")
	l.Set("0", "00000")

	l.Destroy()
}

func Test5(t *testing.T) {
	l := NewList("test.list5")

	var N = 1 << 22
	start := time.Now()

	for i := 0; i < N; i++ {
		l.Set(fmt.Sprintf("%09d", i), ".")
	}

	l.Close()
	fmt.Println("Time to insert", N, "integers:", time.Now().Sub(start))

}

func TestRead(t *testing.T) {
	l := OpenList("test.list5")
	if l == nil {
		t.Error("Couldn't open list")
	}

	iterate(l)
}
