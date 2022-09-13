package histogram

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTable_Add(t *testing.T) {
	h := New()
	h.Add(1 * time.Millisecond)
	h.Add(1234 * time.Microsecond)
	h.Add(4 * time.Second)
	expected := `1ms-2ms: 2
4s-5s: 1

Median: 1ms-2ms
90%: 4s-5s
99%: 4s-5s
99.9%: 4s-5s
`
	s := h.String()
	assert.Equal(t, expected, s)
}

func TestTable_Percentile(t *testing.T) {
	h := New()
	for i := 1; i <= 10; i++ {
		h.Add(time.Duration(i) * time.Millisecond)
	}
	tests := []struct {
		name     string
		percent  float64
		wantMin  time.Duration
		wantHigh time.Duration
	}{
		{"median", 50, 5 * time.Millisecond, 6 * time.Millisecond},
		{"90pc", 90, 9 * time.Millisecond, 10 * time.Millisecond},
		{"99pc", 99, 10 * time.Millisecond, 20 * time.Millisecond},
		{"max", 100, 10 * time.Millisecond, 20 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMin, gotHigh := h.Percentile(tt.percent)
			if gotMin != tt.wantMin {
				t.Errorf("Percentile() gotMin = %v, want %v", gotMin, tt.wantMin)
			}
			if gotHigh != tt.wantHigh {
				t.Errorf("Percentile() gotHigh = %v, want %v", gotHigh, tt.wantHigh)
			}
		})
	}
}

func TestNew(t *testing.T) {
	h := New()
	assert.Equal(t, 72, len(h))
	t.Logf("%s", h[len(h)-1].max)
}

func BenchmarkHist(b *testing.B) {
	h := New()
	for n := 0; n < b.N; n++ {
		h.Add(3456 * time.Microsecond)
	}
	if h.Count() != b.N {
		b.Fail()
	}
}
