package histogram

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

type entry struct {
	min time.Duration
	max time.Duration
	n   int64
}

type Table []entry

func New() Table {
	var t Table
	for delta := 1 * time.Microsecond; delta < 10*time.Second; delta *= 10 {
		for i := time.Duration(1); i < 10; i++ {
			t = append(t, entry{
				min: i * delta,
				max: (i + 1) * delta,
			})
		}
	}
	return t
}

func (t Table) Add(d time.Duration) {
	for i, e := range t {
		if e.min <= d && d < e.max {
			atomic.AddInt64(&(t[i].n), 1)
			return
		}
	}
	log.Println("latency bigger than max in table", d)
}

func (t Table) String() string {
	w := new(strings.Builder)
	for _, e := range t {
		if e.n == 0 {
			continue
		}
		fmt.Fprintf(w, "%s-%s: %d\n", e.min, e.max, e.n)
	}
	return w.String()
}
