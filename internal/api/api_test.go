package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueryInt(t *testing.T) {
	if got := queryInt(httptest.NewRequest("GET", "/x?days=7", nil), "days", 30); got != 7 {
		t.Errorf("queryInt = %d, want 7", got)
	}
	// missing falls back to default
	if got := queryInt(httptest.NewRequest("GET", "/x", nil), "days", 30); got != 30 {
		t.Errorf("missing queryInt = %d, want default 30", got)
	}
	// non-numeric / non-positive falls back to default
	for _, q := range []string{"/x?days=abc", "/x?days=-3", "/x?days=0"} {
		if got := queryInt(httptest.NewRequest("GET", q, nil), "days", 30); got != 30 {
			t.Errorf("queryInt(%q) = %d, want default 30", q, got)
		}
	}
}

func TestParseIntsIgnoresInvalid(t *testing.T) {
	got := parseInts([]string{"1", "abc", "7", ""})
	if len(got) != 2 || got[0] != 1 || got[1] != 7 {
		t.Errorf("parseInts = %v, want [1 7]", got)
	}
}

func TestWriteJSONSetsContentTypeAndStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusTeapot, map[string]any{"ok": true})

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var m map[string]bool
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if !m["ok"] {
		t.Errorf("body = %s, want {\"ok\":true}", rec.Body.String())
	}
}

func TestWriteErr(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErr(rec, errTest("boom"))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }
