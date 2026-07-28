package store

import (
	"fmt"
	"strings"

	"github.com/nbd-wtf/go-nostr"
)

// defaultQueryLimit caps the number of rows a REQ returns when the filter's
// Limit is unspecified or absurd. Archives often raise this; 1000 is a safe default.
const defaultQueryLimit = 1000

// buildFilterSQL turns a nostr.Filter into a WHERE clause + positional args
// (for clickhouse-go's `?` binding) + an ORDER/LIMIT tail. It is a near-1:1
// port of eventstore/postgresql/query.go, adapted to ClickHouse types:
//   - tag predicates use hasAny(Array, Array) on the denormalized tag_* columns
//   - the tombstone predicate `NOT dictHas('tombstone_dict', id)` is always added
//   - replaceable/addressable dedup is handled by the caller via FINAL, not here
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
			for _, v := range vals {
				needle := jsonStringValue(key) + "," + jsonStringValue(v)
				conds = append(conds, "position(tags_raw, ?) > 0")
				args = append(args, needle)
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
	tail = fmt.Sprintf(" ORDER BY created_at DESC, id LIMIT %d", limit)
	return where, args, tail
}

// jsonStringValue returns v quoted as a JSON string with minimal escaping.
func jsonStringValue(v string) string {
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	return "\"" + r.Replace(v) + "\""
}
