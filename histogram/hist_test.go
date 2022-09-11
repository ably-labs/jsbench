package histogram

import (
	"testing"
	"time"
)

func TestTable_Add(t *testing.T) {
	h := New()
	h.Add(1 * time.Millisecond)
	h.Add(1234 * time.Microsecond)
	h.Add(4 * time.Second)
	expected :=
		"1ms-2ms: 2\n" +
			"4s-5s: 1\n"
	s := h.String()
	if s != expected {
		t.Errorf("want %s, got %s", expected, s)
	}
}
