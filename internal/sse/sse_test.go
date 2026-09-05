package sse

import (
	"testing"
)

func collect(t *testing.T, payload string) []string {
	t.Helper()
	var out []string
	Scan([]byte(payload), func(ev Event) bool {
		out = append(out, string(ev.Data))
		return true
	})
	return out
}

func TestScanBasic(t *testing.T) {
	got := collect(t, "data: one\n\ndata: two\n\n")
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("got %v", got)
	}
}

func TestScanCRLF(t *testing.T) {
	got := collect(t, "data: a\r\n\ndata: b\r\n\r\n")
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}

func TestScanDataWithoutSpace(t *testing.T) {
	got := collect(t, "data:x\n\n")
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("got %v", got)
	}
}

func TestScanMultipleDataLinesJoined(t *testing.T) {
	got := collect(t, "data: line1\ndata: line2\n\n")
	if len(got) != 1 || got[0] != "line1\nline2" {
		t.Fatalf("got %v", got)
	}
}

func TestScanIgnoresCommentsAndOtherFields(t *testing.T) {
	got := collect(t, ": keep-alive\nevent: msg\nid: 1\nretry: 100\ndata: real\n\n")
	if len(got) != 1 || got[0] != "real" {
		t.Fatalf("got %v", got)
	}
}

func TestScanTrailingEventWithoutBlankLine(t *testing.T) {
	got := collect(t, "data: a\n\ndata: last")
	if len(got) != 2 || got[1] != "last" {
		t.Fatalf("got %v", got)
	}
}

func TestScanStopCallback(t *testing.T) {
	count := 0
	Scan([]byte("data: 1\n\ndata: 2\n\ndata: 3\n\n"), func(ev Event) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("scan did not stop, count=%d", count)
	}
}
