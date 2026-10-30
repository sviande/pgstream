// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import "strings"

// AddsOnlyExcludedConstraints reports whether stmt is an ALTER TABLE whose
// every action adds a constraint excluded from the target: a foreign key when
// foreignKeys is set, a CHECK constraint when checks is set.
//
// The schema snapshot leaves these constraints out (exclude_foreign_keys,
// exclude_check_constraints), and the live stream has to as well: a foreign key
// on the target rejects a child row that arrives before its parent, and the
// writer skips the rejected row. An ALTER TABLE that also does something else
// is not reported, so it is replayed whole.
func AddsOnlyExcludedConstraints(stmt string, foreignKeys, checks bool) bool {
	if !foreignKeys && !checks {
		return false
	}

	tokens := tokenTexts(sqlTokens([]rune(stmt)))
	if len(tokens) < 2 || tokens[0] != "ALTER" || tokens[1] != "TABLE" {
		return false
	}
	i := 2
	if i+1 < len(tokens) && tokens[i] == "IF" && tokens[i+1] == "EXISTS" {
		i += 2
	}
	if i < len(tokens) && tokens[i] == "ONLY" {
		i++
	}
	// the table name, possibly schema-qualified, and the inheritance star
	i = skipQualifiedName(tokens, i)
	if i < len(tokens) && tokens[i] == "*" {
		i++
	}
	if i >= len(tokens) {
		return false
	}

	for _, action := range splitActions(tokens[i:]) {
		if len(action) == 0 || action[0] != "ADD" || !isExcludedConstraint(action[1:], foreignKeys, checks) {
			return false
		}
	}
	return true
}

// StripExcludedTableConstraints removes from a CREATE TABLE the table
// constraints excluded from the target, foreign keys when foreignKeys is set and
// CHECK constraints when checks is set, as the schema snapshot does. Any other
// statement, and a CREATE TABLE without such a constraint, is returned as is.
// Constraints written in a column definition are left in place.
func StripExcludedTableConstraints(stmt string, foreignKeys, checks bool) string {
	if !foreignKeys && !checks || StatementTag(stmt) != "CREATE TABLE" {
		return stmt
	}

	runes := []rune(stmt)
	tokens := sqlTokens(runes)
	texts := tokenTexts(tokens)
	i := 1
	for i < len(texts) && (texts[i] == "GLOBAL" || texts[i] == "LOCAL" || texts[i] == "TEMP" ||
		texts[i] == "TEMPORARY" || texts[i] == "UNLOGGED") {
		i++
	}
	if i >= len(texts) || texts[i] != "TABLE" {
		return stmt
	}
	i++
	if i+2 < len(texts) && texts[i] == "IF" && texts[i+1] == "NOT" && texts[i+2] == "EXISTS" {
		i += 3
	}
	i = skipQualifiedName(texts, i)
	// CREATE TABLE ... AS, OF type and PARTITION OF have no element list
	if i >= len(texts) || texts[i] != "(" {
		return stmt
	}

	// The element list is "(" segment { "," segment } ")". A segment runs from
	// the separator before it to the one after it, so it keeps the comments
	// and line breaks around its element.
	type segment struct {
		start, end int // rune offsets
		element    []string
	}
	var segments []segment
	depth, from, closing := 0, i, -1
	for j := i; j < len(texts) && closing == -1; j++ {
		switch {
		case texts[j] == "(":
			depth++
			continue
		case texts[j] == ")":
			depth--
			if depth > 0 {
				continue
			}
			closing = j
		case texts[j] != "," || depth != 1:
			continue
		}
		segments = append(segments, segment{start: tokens[from].end, end: tokens[j].start, element: texts[from+1 : j]})
		from = j
	}
	if closing == -1 {
		return stmt
	}

	kept := make([]string, 0, len(segments))
	for _, s := range segments {
		if !isExcludedConstraint(s.element, foreignKeys, checks) {
			kept = append(kept, string(runes[s.start:s.end]))
		}
	}
	if len(kept) == len(segments) {
		return stmt
	}
	return string(runes[:tokens[i].end]) + strings.Join(kept, ",") + string(runes[tokens[closing].start:])
}

// isExcludedConstraint reports whether tokens start a constraint
// [CONSTRAINT name] FOREIGN KEY ... or [CONSTRAINT name] CHECK ..., as
// selected by the flags. FOREIGN and CHECK are reserved words, so a column with
// one of these names is quoted and does not match.
func isExcludedConstraint(tokens []string, foreignKeys, checks bool) bool {
	i := 0
	if i < len(tokens) && tokens[i] == "CONSTRAINT" {
		i += 2
	}
	if i >= len(tokens) {
		return false
	}
	switch tokens[i] {
	case "FOREIGN":
		return foreignKeys && i+1 < len(tokens) && tokens[i+1] == "KEY"
	case "CHECK":
		return checks
	}
	return false
}

// skipQualifiedName returns the position after the possibly schema-qualified
// name starting at i.
func skipQualifiedName(tokens []string, i int) int {
	i++
	if i+1 < len(tokens) && tokens[i] == "." {
		i += 2
	}
	return i
}

// splitActions splits the actions of an ALTER TABLE on the commas outside
// parentheses.
func splitActions(tokens []string) [][]string {
	var actions [][]string
	depth, start := 0, 0
	for i, tok := range tokens {
		switch tok {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				actions = append(actions, tokens[start:i])
				start = i + 1
			}
		}
	}
	return append(actions, tokens[start:])
}

// sqlToken is a token of a statement with its position, in runes.
type sqlToken struct {
	text       string
	start, end int
}

// sqlTokens splits a statement into tokens, leaving out whitespace and
// comments: unquoted words uppercased, quoted identifiers, string literals and
// dollar-quoted bodies as they are, and every other character on its own.
func sqlTokens(runes []rune) []sqlToken {
	var tokens []sqlToken
	add := func(start, end int, text string) {
		tokens = append(tokens, sqlToken{text: text, start: start, end: end})
	}
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case r == ' ' || r == '\t' || r == '\r' || r == '\n':
			i++
		case r == '-' && i+1 < len(runes) && runes[i+1] == '-':
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
		case r == '/' && i+1 < len(runes) && runes[i+1] == '*':
			end := indexRunes(runes, i+2, "*/")
			if end == -1 {
				return tokens
			}
			i = end + 2
		case r == '"' || r == '\'':
			j := i + 1
			for j < len(runes) {
				if runes[j] == r {
					if j+1 < len(runes) && runes[j+1] == r {
						j += 2 // doubled quote
						continue
					}
					break
				}
				j++
			}
			end := min(j+1, len(runes))
			add(i, end, string(runes[i:end]))
			i = end
		case r == '$' && dollarQuoteTag(runes, i) != "":
			tag := dollarQuoteTag(runes, i)
			end := indexRunes(runes, i+len(tag), tag)
			if end == -1 {
				add(i, len(runes), string(runes[i:]))
				return tokens
			}
			add(i, end+len(tag), string(runes[i:end+len(tag)]))
			i = end + len(tag)
		case isWordRune(r):
			j := i
			for j < len(runes) && (isWordRune(runes[j]) || runes[j] == '$') {
				j++
			}
			add(i, j, strings.ToUpper(string(runes[i:j])))
			i = j
		default:
			add(i, i+1, string(r))
			i++
		}
	}
	return tokens
}

func tokenTexts(tokens []sqlToken) []string {
	texts := make([]string, len(tokens))
	for i, t := range tokens {
		texts[i] = t.text
	}
	return texts
}

// indexRunes returns the index of the first occurrence of sub in runes at or
// after from, or -1. sub is ASCII, so its length in bytes is its length in
// runes.
func indexRunes(runes []rune, from int, sub string) int {
	s := []rune(sub)
	for i := from; i+len(s) <= len(runes); i++ {
		if string(runes[i:i+len(s)]) == sub {
			return i
		}
	}
	return -1
}

func isWordRune(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r > 127
}
