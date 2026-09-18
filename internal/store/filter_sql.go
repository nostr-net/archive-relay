package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nbd-wtf/go-nostr"
)

// defaultQueryLimit caps the number of rows a REQ returns when the filter's
// Limit is unspecified or absurd. Archives often raise this; 1000 is a safe default.
const defaultQueryLimit = 1000

// collapseLimitBy is the QueryEvents tail fragment that collapses replaceable
// versions and exact-id duplicates. kind MUST be in the group key: without it
// an author's kind-0 profile and kind-3 contacts share the pubkey key and one
// disappears (codex BLOCKER 1).
//
// Semantic: "latest matching version" — this runs AFTER WHERE, so a tag/until
// filter can return an older version when the newest does not match.
const collapseLimitBy = "LIMIT 1 BY kind, if(kind IN (0,3,10002), pubkey, id)"

// buildFilterSQL turns a nostr.Filter into a WHERE clause + positional args
// (for clickhouse-go's `?` binding) + an ORDER/LIMIT-BY/LIMIT tail. It is a
// near-1:1 port of eventstore/postgresql/query.go, adapted to ClickHouse types:
//   - tag predicates use hasAny(Array, Array) on the denormalized tag_* columns
//   - the tombstone predicate `NOT dictHas('tombstone_dict', id)` is always added
//   - replaceable collapse is the tail's LIMIT 1 BY (QueryEvents); CountEvents
//     ignores the tail and keeps FINAL (§1.10)
//
// The hot single-letter tags e/p/t/d are first-class; arbitrary tag keys (r, a,
// custom) fall back to a tags_raw substring scan (slower, but rare).
func buildFilterSQL(f nostr.Filter) (where string, args []any, tail string) {
	var conds []string

	if len(f.IDs) > 0 {
		conds = append(conds, "id IN (?)")
		args = append(args, f.IDs) // clickhouse-go binds a slice to IN (?)
	}
	if len(f.Authors) > 0 {
		conds = append(conds, "pubkey IN (?)")
		args = append(args, f.Authors)
	}
	if len(f.Kinds) > 0 {
		conds = append(conds, "kind IN (?)")
		kinds := make([]int32, len(f.Kinds))
		for i, k := range f.Kinds {
			kinds[i] = int32(k)
		}
		args = append(args, kinds)
	}
	for key, vals := range f.Tags {
		if len(vals) == 0 {
			continue
		}
		switch key {
		case "e":
			conds = append(conds, "hasAny(tag_e, ?)")
			args = append(args, vals)
		case "p":
			conds = append(conds, "hasAny(tag_p, ?)")
			args = append(args, vals)
		case "t":
			conds = append(conds, "hasAny(tag_t, ?)")
			args = append(args, vals)
		case "d":
			// d-tag is single-valued; match the first requested value
			conds = append(conds, "tag_d = ?")
			args = append(args, vals[0])
		default:
			// arbitrary key: best-effort substring scan over tags_raw JSON.
			// e.g. key="r", val="wss://x" -> position(tags_raw, '"r","wss://x"') > 0
			// marshal via encoding/json so the escaping matches tags_raw exactly.
			kb, _ := json.Marshal(key)
			for _, v := range vals {
				vb, _ := json.Marshal(v)
				conds = append(conds, "position(tags_raw, ?) > 0")
				args = append(args, string(kb)+","+string(vb))
			}
		}
	}
	if f.Since != nil {
		conds = append(conds, "created_at >= ?")
		args = append(args, uint32(*f.Since))
	}
	if f.Until != nil {
		conds = append(conds, "created_at <= ?")
		args = append(args, uint32(*f.Until))
	}

	// Always exclude tombstoned events (NIP-09 / moderation). dictHas is O(1).
	conds = append(conds, "NOT dictHas('tombstone_dict', id)")

	if len(conds) == 0 {
		where = "1=1"
	} else {
		where = strings.Join(conds, " AND ")
	}

	limit := f.Limit
	if limit < 1 || limit > defaultQueryLimit {
		limit = defaultQueryLimit
	}
	tail = fmt.Sprintf(" ORDER BY created_at DESC, id ASC %s LIMIT %d", collapseLimitBy, limit)
	return where, args, tail
}
