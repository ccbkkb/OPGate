package canon

import (
	"testing"
)

func TestCanonicalizeSortsObjectKeys(t *testing.T) {
	out, err := Canonicalize([]byte(`{"b":1,"a":{"d":4,"c":[3,1,2]}}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":{"c":[3,1,2],"d":4},"b":1}`
	if string(out) != want {
		t.Fatalf("got %s want %s", out, want)
	}
}

func TestCanonicalizePreservesArrayOrder(t *testing.T) {
	a, _ := Canonicalize([]byte(`[1,2,3]`))
	b, _ := Canonicalize([]byte(`[3,2,1]`))
	if string(a) == string(b) {
		t.Fatal("array order must matter")
	}
}

func TestCanonicalizePreservesNumberLexicalForm(t *testing.T) {
	out, err := Canonicalize([]byte(`{"a":1.50,"b":1e2,"c":3}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":1.50,"b":1e2,"c":3}`
	if string(out) != want {
		t.Fatalf("got %s want %s", out, want)
	}
}

func TestCanonicalizeNullVsMissing(t *testing.T) {
	withNull, _ := Canonicalize([]byte(`{"a":null}`))
	missing, _ := Canonicalize([]byte(`{}`))
	if string(withNull) == string(missing) {
		t.Fatal("explicit null and missing field must hash differently")
	}
}

func TestCanonicalizeStringEscaping(t *testing.T) {
	// raw UTF-8 stays literal; only quote/backslash/control chars escaped
	out, err := Canonicalize([]byte(`{"s":"quote\" back\\ nl\n tab\t unicode你好"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"s":"quote\" back\\ nl\n tab\t unicode你好"}`
	if string(out) != want {
		t.Fatalf("got %s want %s", out, want)
	}
}

func TestCanonicalizeControlCharsEscaped(t *testing.T) {
	// a JSON-escaped control char survives the canonical round trip
	out, err := Canonicalize([]byte(`{"s":"a\u0001b"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"s":"a\u0001b"}`
	if string(out) != want {
		t.Fatalf("got %s want %s", out, want)
	}
}

func TestCanonicalizeRejectsInvalidJSON(t *testing.T) {
	if _, err := Canonicalize([]byte(`{nope`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestSameSemanticMessagesSameHash(t *testing.T) {
	// whitespace and key order differ; semantics identical
	a, err := Canonicalize([]byte(`{"role":"user",  "content":"Hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Canonicalize([]byte(`{"content":"Hi","role":"user"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("canonical forms differ: %s vs %s", a, b)
	}
	if HashBytes(a) != HashBytes(b) {
		t.Fatal("hashes differ for semantically identical messages")
	}
}

func TestHashMessagesArrayFraming(t *testing.T) {
	m1, _ := Canonicalize([]byte(`{"role":"user","content":"U1"}`))
	m2, _ := Canonicalize([]byte(`{"role":"assistant","content":"A1"}`))

	h1 := HistoryHash([][]byte{m1})
	h12 := HistoryHash([][]byte{m1, m2})
	if h1 == h12 {
		t.Fatal("different states must hash differently")
	}

	// FullState appends the assistant message
	full := FullState([][]byte{m1}, m2)
	wantHash := HashBytes(MessageArray([][]byte{m1, m2}))
	if HashBytes(full) != wantHash {
		t.Fatal("FullState mismatch")
	}
}

func TestShort(t *testing.T) {
	if got := Short("0123456789abcdef"); got != "0123456789ab" {
		t.Fatalf("got %s", got)
	}
	if got := Short("abc"); got != "abc" {
		t.Fatalf("got %s", got)
	}
}
