package histogram

// histogram is a semi logarithmic histogram which gives 1 digit of precison.
// Buckets start 1us, 2us, 3us, ... 20us, 30us
// It can record times up to 2h and takes under 10ns to record a value.

import (
	"fmt"
	"io"
	"log"
	"math"
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
	for delta := 100 * time.Microsecond; delta < 10000*time.Second; delta *= 10 {
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
		if d < e.max {
			atomic.AddInt64(&(t[i].n), 1)
			return
		}
	}
	// We have a duration longer than 2h 46mins. Just ignore it.
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
	fmt.Fprintln(w)
	t.printPercentile(w, "Median", 50)
	t.printPercentile(w, "90%", 90)
	t.printPercentile(w, "99%", 99)
	t.printPercentile(w, "99.9%", 99.9)

	return w.String()
}

func (t Table) printPercentile(w io.Writer, prefix string, percent float64) {
	min, max := t.Percentile(percent)
	fmt.Fprintf(w, "%s: %s-%s\n", prefix, min, max)
}

func (t Table) Count() int {
	n := 0
	for _, e := range t {
		n += int(e.n)
	}
	return n
}

func (t Table) nth(want int) (min time.Duration, high time.Duration) {
	n := 0
	for _, e := range t {
		n += int(e.n)
		if n >= want {
			return e.min, e.max
		}
	}
	return 0, 0
}

func (t Table) Percentile(percent float64) (min time.Duration, high time.Duration) {
	n := int(math.Ceil(percent / 100 * float64(t.Count())))
	return t.nth(n)
}
