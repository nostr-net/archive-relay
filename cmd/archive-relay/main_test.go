package main

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSplitSources(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"wss://a", []string{"wss://a"}},
		{"wss://a,wss://b,wss://c", []string{"wss://a", "wss://b", "wss://c"}},
		{"wss://a,,wss://b,", []string{"wss://a", "wss://b"}}, // empties skipped
		{",,,", nil},
	}
	for _, c := range cases {
		got := splitSources(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitSources(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestSplitSourcesRoundtrip(t *testing.T) {
	want := []string{"wss://relay.damus.io", "wss://nos.lol", "wss://relay.primal.net"}
	joined := ""
	for i, s := range want {
		if i > 0 {
			joined += ","
		}
		joined += s
	}
	got := splitSources(joined)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("roundtrip mismatch: got %v want %v", got, want)
	}
}
