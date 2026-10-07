package query

import (
	"errors"
	"testing"
)

func TestCursorRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, 50, 1 << 40} {
		got, err := decodeCursor(encodeCursor(v))
		if err != nil || got != v {
			t.Errorf("v=%d: got %d err=%v", v, got, err)
		}
	}
	if v, err := decodeCursor(""); err != nil || v != 0 {
		t.Errorf("cursor vazio: %d %v", v, err)
	}
}

func TestCursorRejectsGarbage(t *testing.T) {
	for _, c := range []string{"!!!", "bm90LWpzb24", "eyJ2IjotMX0" /* {"v":-1} */, "eyJ2Ijoic3RyIn0" /* {"v":"str"} */} {
		if _, err := decodeCursor(c); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("cursor %q: err = %v", c, err)
		}
	}
}
