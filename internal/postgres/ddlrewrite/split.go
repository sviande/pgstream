// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import "strings"

// SplitStatements splits a raw SQL string into its individual statements.
//
// The DDL captured by the pgstream event trigger is current_query(): the whole
// query text the client sent, which for a migration tool such as Prisma is the
// entire migration file, not a single command. Rewriting that text as if it
// were one statement mis-handles every statement but the first, so callers
// split first and rewrite each statement on its own.
//
// The scanner respects string literals, quoted identifiers, line and block
// comments, and dollar-quoted bodies (function definitions), so a semicolon
// inside any of those does not end a statement. Leading comments stay attached
// to the statement that follows them.
func SplitStatements(sql string) []string {
	var (
		statements []string
		current    strings.Builder
	)

	const (
		normal = iota
		inSingleQuote
		inDoubleQuote
		inLineComment
		inBlockComment
		inDollarQuote
	)

	state := normal
	dollarTag := ""
	runes := []rune(sql)

	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		switch state {
		case inSingleQuote:
			current.WriteRune(ch)
			if ch == '\'' {
				// '' is an escaped quote, not the end of the literal
				if i+1 < len(runes) && runes[i+1] == '\'' {
					i++
					current.WriteRune(runes[i])
					continue
				}
				state = normal
			}
			continue

		case inDoubleQuote:
			current.WriteRune(ch)
			if ch == '"' {
				if i+1 < len(runes) && runes[i+1] == '"' {
					i++
					current.WriteRune(runes[i])
					continue
				}
				state = normal
			}
			continue

		case inLineComment:
			current.WriteRune(ch)
			if ch == '\n' {
				state = normal
			}
			continue

		case inBlockComment:
			current.WriteRune(ch)
			if ch == '*' && i+1 < len(runes) && runes[i+1] == '/' {
				i++
				current.WriteRune(runes[i])
				state = normal
			}
			continue

		case inDollarQuote:
			current.WriteRune(ch)
			if ch == '$' && strings.HasPrefix(string(runes[i:]), dollarTag) {
				for _, r := range dollarTag[1:] {
					i++
					_ = r
					current.WriteRune(runes[i])
				}
				state = normal
				dollarTag = ""
			}
			continue
		}

		// state == normal
		switch {
		case ch == '\'':
			state = inSingleQuote
		case ch == '"':
			state = inDoubleQuote
		case ch == '-' && i+1 < len(runes) && runes[i+1] == '-':
			state = inLineComment
		case ch == '/' && i+1 < len(runes) && runes[i+1] == '*':
			state = inBlockComment
		case ch == '$':
			if tag := dollarQuoteTag(runes, i); tag != "" {
				dollarTag = tag
				state = inDollarQuote
				current.WriteString(tag)
				i += len(tag) - 1
				continue
			}
		case ch == ';':
			current.WriteRune(ch)
			if stmt := strings.TrimSpace(current.String()); stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// dollarQuoteTag returns the dollar-quote tag starting at position i ("$$",
// "$body$", ...), or "" when the dollar sign does not open one.
func dollarQuoteTag(runes []rune, i int) string {
	if runes[i] != '$' {
		return ""
	}
	for j := i + 1; j < len(runes); j++ {
		switch {
		case runes[j] == '$':
			return string(runes[i : j+1])
		case runes[j] == '_' || runes[j] >= 'a' && runes[j] <= 'z' ||
			runes[j] >= 'A' && runes[j] <= 'Z' || runes[j] >= '0' && runes[j] <= '9':
			// tag characters, keep scanning
		default:
			return ""
		}
	}
	return ""
}

// StatementTag derives the command tag of a single statement, skipping any
// leading comments. It is a best-effort match on the leading keywords: only the
// type-related tags drive rewriting decisions, everything else falls through to
// the generic conversion path.
func StatementTag(stmt string) string {
	words := leadingWords(stmt, 2)
	if len(words) == 0 {
		return ""
	}
	switch words[0] {
	case "CREATE", "ALTER", "DROP":
		if len(words) > 1 {
			return words[0] + " " + words[1]
		}
	}
	return words[0]
}

// leadingWords returns up to n leading uppercase words of a statement, skipping
// leading whitespace and comments.
func leadingWords(stmt string, n int) []string {
	var words []string
	rest := stmt
	for len(words) < n {
		rest = strings.TrimLeft(rest, " \t\r\n")
		switch {
		case strings.HasPrefix(rest, "--"):
			if idx := strings.IndexByte(rest, '\n'); idx != -1 {
				rest = rest[idx+1:]
				continue
			}
			return words
		case strings.HasPrefix(rest, "/*"):
			if idx := strings.Index(rest, "*/"); idx != -1 {
				rest = rest[idx+2:]
				continue
			}
			return words
		}
		if rest == "" {
			return words
		}
		end := strings.IndexAny(rest, " \t\r\n(;")
		if end == -1 {
			end = len(rest)
		}
		word := strings.ToUpper(rest[:end])
		if word != "" {
			words = append(words, word)
		}
		if end == len(rest) {
			return words
		}
		rest = rest[end:]
	}
	return words
}
