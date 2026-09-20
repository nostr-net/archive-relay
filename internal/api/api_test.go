package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nostr-net/archive-relay/internal/policy"
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

func TestEventsFilterBreadth(t *testing.T) {
	q := url.Values{"kind": {"1", "invalid", "3"}}
	for i := range 1500 {
		q.Add("id", strconv.Itoa(i))
		q.Add("author", strconv.Itoa(i))
	}
	for _, tc := range []struct {
		name         string
		breadth      policy.RejectFilterBreadth
		ids, authors int
	}{
		{"both capped", policy.RejectFilterBreadth{MaxIDs: 1000, MaxAuthors: 500}, 1000, 500},
		{"ids unlimited", policy.RejectFilterBreadth{MaxAuthors: 500}, 1500, 500},
		{"authors unlimited", policy.RejectFilterBreadth{MaxIDs: 1000}, 1000, 1500},
		{"both unlimited", policy.RejectFilterBreadth{}, 1500, 1500},
		{"below caps", policy.RejectFilterBreadth{MaxIDs: 2000, MaxAuthors: 2000}, 1500, 1500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := eventsFilter(q, tc.breadth)
			if !reflect.DeepEqual(f.IDs, q["id"][:tc.ids]) || !reflect.DeepEqual(f.Authors, q["author"][:tc.authors]) {
				t.Fatalf("filter did not preserve the expected prefixes: ids=%d authors=%d", len(f.IDs), len(f.Authors))
			}
			if !reflect.DeepEqual(f.Kinds, []int{1, 3}) {
				t.Errorf("kinds = %v, want [1 3]", f.Kinds)
			}
		})
	}
}

func TestEventsFilterLimit(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", 100}, {"invalid", 100}, {"0", 100}, {"-1", 100},
		{"1", 1}, {"999", 999}, {"1000", 1000}, {"1500", 1000},
	} {
		t.Run(tc.value, func(t *testing.T) {
			f := eventsFilter(url.Values{"limit": {tc.value}}, policy.RejectFilterBreadth{})
			if f.Limit != tc.want {
				t.Errorf("limit = %d, want %d", f.Limit, tc.want)
			}
		})
	}
}

type healthStore struct {
	eventStore
	conn driver.Conn
}

func (s healthStore) CH() driver.Conn { return s.conn }

func (s healthStore) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

type healthConn struct {
	driver.Conn
	pingErr  error
	pingCtx  context.Context
	queryCtx context.Context
	query    string
}

func (c *healthConn) Ping(ctx context.Context) error {
	c.pingCtx = ctx
	return c.pingErr
}

func (c *healthConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.queryCtx, c.query = ctx, query
	return healthRow{}
}

type healthRow struct{ driver.Row }

func (healthRow) Scan(dest ...any) error {
	*dest[0].(*uint64) = 42
	return nil
}

func TestHealthPhysicalRows(t *testing.T) {
	for _, healthy := range []bool{true, false} {
		t.Run(strconv.FormatBool(healthy), func(t *testing.T) {
			conn := &healthConn{}
			if !healthy {
				conn.pingErr = errTest("unavailable")
			}
			h := &Handler{store: healthStore{conn: conn}}
			mux := http.NewServeMux()
			h.Register(mux)
			rec := httptest.NewRecorder()
			start := time.Now()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/health", nil))
			var body struct {
				OK         bool   `json:"ok"`
				ClickHouse bool   `json:"clickhouse"`
				Events     uint64 `json:"events"`
				Note       string `json:"events_note"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			wantStatus, wantEvents := http.StatusOK, uint64(42)
			if !healthy {
				wantStatus, wantEvents = http.StatusServiceUnavailable, 0
			}
			if rec.Code != wantStatus || body.OK != healthy || body.ClickHouse != healthy || body.Events != wantEvents {
				t.Errorf("status=%d body=%+v, want status=%d healthy=%t events=%d", rec.Code, body, wantStatus, healthy, wantEvents)
			}
			if body.Note != "physical rows (size gauge, includes duplicates/tombstoned)" {
				t.Errorf("unexpected events_note: %q", body.Note)
			}
			deadline, ok := conn.pingCtx.Deadline()
			if !ok || deadline.Before(start.Add(2*time.Second)) || deadline.After(time.Now().Add(2*time.Second)) {
				t.Errorf("expected 2s timeout, got %v", deadline)
			}
			if healthy {
				wantQuery := "SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('events_permanent','events_archive','events_social')"
				if strings.Join(strings.Fields(conn.query), " ") != wantQuery {
					t.Errorf("query = %q", conn.query)
				}
				if conn.queryCtx != conn.pingCtx {
					t.Error("ping and query must share the timeout context")
				}
			} else if conn.query != "" {
				t.Error("queried rows after failed ping")
			}
		})
	}
}
