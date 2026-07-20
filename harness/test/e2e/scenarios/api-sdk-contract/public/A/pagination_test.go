package pagination

import "testing"

func TestCursorRoundTrip(t *testing.T) {
	want := Cursor{Offset: 40}
	token, err := EncodeCursor(want, []byte("test-only-signing-key"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCursor(token, []byte("test-only-signing-key"))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
