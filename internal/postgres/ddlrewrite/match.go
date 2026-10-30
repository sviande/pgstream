// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"slices"
	"strings"
)

// MatchStatement finds the statement of a captured query that fired a DDL
// event, among statements[from:]. It returns the index of that statement and
// false when none can be told with certainty.
//
// The event trigger captures current_query(), the whole text the client sent,
// and fires once per DDL command, so the statement is not in the event: it is
// the next one in text order that carries the event's command tag and names
// one of the objects the event affects (identities as given by
// pg_event_trigger_ddl_commands). Statements skipped over did not fire an
// event of their own (transaction control, data changes, DDL on temporary
// objects). Without an object named in the text, as with an index Postgres
// named itself, the tag alone is accepted when a single statement carries it.
func MatchStatement(statements []string, from int, commandTag string, identities []string) (int, bool) {
	names := make([]string, 0, len(identities))
	for _, identity := range identities {
		names = append(names, identityNames(identity)...)
	}

	var tagged []int
	for i := from; i < len(statements); i++ {
		if !hasCommandTag(statements[i], commandTag) {
			continue
		}
		if len(names) == 0 || slices.ContainsFunc(names, func(name string) bool {
			return mentionsIdentifier(statements[i], name)
		}) {
			return i, true
		}
		tagged = append(tagged, i)
	}
	if len(tagged) == 1 {
		return tagged[0], true
	}
	return -1, false
}

// hasCommandTag reports whether a statement is one Postgres gives the command
// tag of an event trigger.
func hasCommandTag(stmt, commandTag string) bool {
	tagWords := strings.Fields(strings.ToUpper(commandTag))
	// tags that name the statement after what it does rather than after its
	// leading keywords
	switch strings.Join(tagWords, " ") {
	case "CREATE TABLE AS":
		tagWords = []string{"CREATE", "TABLE"}
	case "SELECT INTO":
		tagWords = []string{"SELECT"}
	}
	return len(tagWords) > 0 && slices.Equal(commandWords(stmt, len(tagWords)), tagWords)
}

// identityNames returns the unqualified names an object identity is written
// with in a statement: the object's own and, for an object defined on a table
// (trigger, constraint, policy, rule: "name on schema.table"), the table's.
// Function arguments are left out.
func identityNames(identity string) []string {
	var names []string
	for _, part := range strings.Split(identity, " on ") {
		if i := strings.IndexByte(part, '('); i != -1 {
			part = part[:i]
		}
		if name := lastIdentifier(strings.TrimSpace(part)); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// lastIdentifier returns the last component of a qualified name, unquoted.
func lastIdentifier(qualified string) string {
	if !strings.HasSuffix(qualified, `"`) {
		return qualified[strings.LastIndexByte(qualified, '.')+1:]
	}
	// walk back to the opening quote, a doubled quote being an escaped one
	for i := len(qualified) - 2; i >= 0; i-- {
		if qualified[i] != '"' {
			continue
		}
		if i > 0 && qualified[i-1] == '"' {
			i--
			continue
		}
		return strings.ReplaceAll(qualified[i+1:len(qualified)-1], `""`, `"`)
	}
	return ""
}

// mentionsIdentifier reports whether a statement holds name as a whole
// identifier, ignoring case: an unquoted identifier is folded to lower case,
// so the statement may spell it in any case.
func mentionsIdentifier(stmt, name string) bool {
	stmt, name = strings.ToLower(stmt), strings.ToLower(name)
	for offset := 0; ; {
		i := strings.Index(stmt[offset:], name)
		if i == -1 {
			return false
		}
		start, end := offset+i, offset+i+len(name)
		if (start == 0 || !isIdentifierByte(stmt[start-1])) && (end == len(stmt) || !isIdentifierByte(stmt[end])) {
			return true
		}
		offset = start + 1
	}
}

func isIdentifierByte(b byte) bool {
	return b == '_' || b == '$' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b >= 0x80
}
