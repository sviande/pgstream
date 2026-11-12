// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import "strings"

// FiringStatement is a statement of a captured query that can fire DDL events.
type FiringStatement struct {
	SQL string
	// InBlock marks a statement written in the body of a DO block: the
	// procedural code around it decides whether it runs, so it may not have.
	InBlock bool
	// Run numbers the sequence a statement of a DO block belongs to: statements
	// written one right after the other, with no control statement, block or
	// label in between, share it. The code runs such a sequence in text order
	// and cannot reach a statement without having run the ones before it. It is
	// zero outside a block and unique within a captured query.
	Run int
	// Opaque marks a DO block whose DDL cannot be read from its text: dynamic
	// SQL (EXECUTE), a language other than PL/pgSQL, a body that cannot be
	// found. SQL holds the whole block.
	Opaque bool
}

// SplitFiringStatements splits a captured query into the statements that can
// fire its DDL events, in text order. A DO block is replaced by the SQL
// statements written in its PL/pgSQL body: the event trigger captures the
// top-level text only, so DDL run by the block (a migration adding a foreign
// key under IF NOT EXISTS and a lock timeout, say) matches none of the
// top-level statements. A block whose DDL cannot be read stays as one opaque
// statement.
func SplitFiringStatements(sql string) []FiringStatement {
	var (
		statements []FiringStatement
		runs       int
	)
	for _, stmt := range SplitStatements(sql) {
		if !isDOBlock(stmt) {
			statements = append(statements, FiringStatement{SQL: stmt})
			continue
		}
		body, ok := doBlockStatements(stmt)
		if !ok {
			statements = append(statements, FiringStatement{SQL: stmt, Opaque: true})
			continue
		}
		last := runs
		for _, s := range body {
			statements = append(statements, FiringStatement{SQL: s.sql, InBlock: true, Run: runs + s.run})
			last = runs + s.run
		}
		runs = last
	}
	return statements
}

func isDOBlock(stmt string) bool {
	words := leadingWords(stmt, 1)
	return len(words) == 1 && words[0] == "DO"
}

// doBlockStatements returns the SQL statements written in the PL/pgSQL body of
// a DO block, in text order, and false when the DDL of the block cannot be
// read from its text.
func doBlockStatements(stmt string) ([]plStatement, bool) {
	body, ok := doBlockBody(stmt)
	if !ok {
		return nil, false
	}
	return plpgsqlStatements(body)
}

// doBlockBody returns the code of a DO block: DO [LANGUAGE lang] code
// [LANGUAGE lang]. It fails for a language other than PL/pgSQL, the default.
func doBlockBody(stmt string) (string, bool) {
	tokens := lexPL([]rune(stmt))
	if len(tokens) == 0 || !tokens[0].isWord("DO") {
		return "", false
	}
	var (
		body     string
		haveBody bool
		language = "plpgsql"
	)
	for i := 1; i < len(tokens); i++ {
		tok := tokens[i]
		switch {
		case tok.isWord("LANGUAGE") && i+1 < len(tokens):
			i++
			language = strings.ToLower(tokens[i].value)
		case tok.kind == tokString && !haveBody:
			body, haveBody = tok.value, true
		case tok.isPunct(';'):
		default:
			return "", false
		}
	}
	return body, haveBody && language == "plpgsql"
}

// plpgsqlStatements returns the SQL statements of a PL/pgSQL block, in text
// order: whatever starts where PL/pgSQL expects a statement and is not one of
// its own control statements. The block is not parsed: past BEGIN, THEN, ELSE
// and LOOP, the next word starts a statement, which ends at its semicolon. The
// conditions of IF, WHILE, FOR and WHEN, and expressions in parentheses, are
// skipped. EXECUTE runs a text built at run time, so a block holding one is
// left unread.
//
// Each statement comes with the number of its run, counted from one: a
// statement starting right after the semicolon of the previous one continues
// its run, anything in between (a control statement, a block keyword, a label)
// starts another.
func plpgsqlStatements(body string) ([]plStatement, bool) {
	runes := []rune(body)
	tokens := lexPL(runes)

	var (
		statements []plStatement
		depth      int
		start      = -1 // first token of the SQL statement being read
		atStart    = true
		inEnd      = false // END IF, END LOOP, END label: skipped to its semicolon
		run        int
		lastEnd    = -2 // semicolon of the last SQL statement read
	)
	begin := func(i int) {
		if i != lastEnd+1 {
			run++
		}
		start, atStart = i, false
	}
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		switch {
		case tok.isPunct('('):
			depth++
			continue
		case tok.isPunct(')'):
			depth--
			continue
		case depth > 0:
			continue
		case tok.isPunct(';'):
			if start >= 0 {
				statements = append(statements, plStatement{sql: strings.TrimSpace(string(runes[tokens[start].start:tok.end])), run: run})
				lastEnd = i
			}
			start, atStart, inEnd = -1, true, false
			continue
		case start >= 0 || inEnd:
			continue
		case atStart && tok.isPunct('<') && i+1 < len(tokens) && tokens[i+1].isPunct('<'):
			i = labelEnd(tokens, i)
			continue
		case tok.kind != tokWord:
			if atStart {
				// an assignment target written as a quoted identifier, say
				begin(i)
			}
			continue
		}

		word := strings.ToUpper(tok.value)
		if word == "EXECUTE" {
			return nil, false
		}
		if !atStart {
			if _, ok := plpgsqlOpeners[word]; ok {
				atStart = true
			}
			continue
		}
		switch _, opener := plpgsqlOpeners[word]; {
		case opener:
		case word == "END":
			inEnd = true
			atStart = false
		default:
			if _, ok := plpgsqlControl[word]; ok {
				atStart = false
				continue
			}
			begin(i)
		}
	}
	if start >= 0 {
		statements = append(statements, plStatement{sql: strings.TrimSpace(string(runes[tokens[start].start:])), run: run})
	}
	return statements, true
}

// plStatement is a SQL statement of a PL/pgSQL block, with the number of its
// run.
type plStatement struct {
	sql string
	run int
}

// labelEnd returns the position of the closing >> of a <<label>> whose
// opening << starts at i.
func labelEnd(tokens []plToken, i int) int {
	for j := i + 2; j < len(tokens); j++ {
		if tokens[j-1].isPunct('>') && tokens[j].isPunct('>') {
			return j
		}
	}
	return len(tokens)
}

// plpgsqlOpeners are the PL/pgSQL keywords after which a statement starts.
var plpgsqlOpeners = map[string]struct{}{
	"BEGIN": {}, "DECLARE": {}, "THEN": {}, "ELSE": {}, "LOOP": {},
}

// plpgsqlControl are the PL/pgSQL statements that are not SQL: what follows
// them up to the next opener or semicolon is not a statement of its own.
var plpgsqlControl = map[string]struct{}{
	"IF": {}, "ELSIF": {}, "ELSEIF": {}, "CASE": {}, "WHEN": {}, "WHILE": {}, "FOR": {}, "FOREACH": {},
	"EXCEPTION": {}, "EXIT": {}, "CONTINUE": {}, "RETURN": {}, "RAISE": {}, "ASSERT": {}, "PERFORM": {},
	"GET": {}, "OPEN": {}, "FETCH": {}, "MOVE": {}, "CLOSE": {}, "NULL": {},
}

type plTokenKind int

const (
	tokWord plTokenKind = iota
	tokString
	tokQuotedIdent
	tokPunct
)

// plToken is a lexical token, with its position in runes.
type plToken struct {
	kind plTokenKind
	// value is the word, the contents of a string literal (quotes removed,
	// escapes resolved) or the punctuation character.
	value      string
	start, end int
}

func (t plToken) isWord(w string) bool { return t.kind == tokWord && strings.EqualFold(t.value, w) }

func (t plToken) isPunct(c rune) bool { return t.kind == tokPunct && t.value == string(c) }

// lexPL splits SQL or PL/pgSQL text into words, string literals (single,
// escape and dollar quoted), quoted identifiers and punctuation, dropping
// whitespace and comments.
func lexPL(runes []rune) []plToken {
	var tokens []plToken
	for i := 0; i < len(runes); {
		ch := runes[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '\f':
			i++
		case ch == '-' && i+1 < len(runes) && runes[i+1] == '-':
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
		case ch == '/' && i+1 < len(runes) && runes[i+1] == '*':
			// block comments nest in Postgres
			nesting := 0
			for i < len(runes) {
				if runes[i] == '/' && i+1 < len(runes) && runes[i+1] == '*' {
					nesting++
					i += 2
					continue
				}
				if runes[i] == '*' && i+1 < len(runes) && runes[i+1] == '/' {
					nesting--
					i += 2
					if nesting == 0 {
						break
					}
					continue
				}
				i++
			}
		case ch == '\'' || (ch == 'E' || ch == 'e') && i+1 < len(runes) && runes[i+1] == '\'':
			start := i
			escapes := ch != '\''
			if escapes {
				i++
			}
			value, end := quoted(runes, i, '\'', escapes)
			tokens = append(tokens, plToken{kind: tokString, value: value, start: start, end: end})
			i = end
		case ch == '"':
			value, end := quoted(runes, i, '"', false)
			tokens = append(tokens, plToken{kind: tokQuotedIdent, value: value, start: i, end: end})
			i = end
		case ch == '$' && dollarQuoteTag(runes, i) != "":
			tag := dollarQuoteTag(runes, i)
			n := len([]rune(tag))
			open := i + n
			// an unterminated body runs to the end of the text
			closing, end := len(runes), len(runes)
			for j := open; j+n <= len(runes); j++ {
				if string(runes[j:j+n]) == tag {
					closing, end = j, j+n
					break
				}
			}
			tokens = append(tokens, plToken{kind: tokString, value: string(runes[open:closing]), start: i, end: end})
			i = end
		case isPLWordRune(ch):
			start := i
			for i < len(runes) && (isPLWordRune(runes[i]) || runes[i] == '$') {
				i++
			}
			tokens = append(tokens, plToken{kind: tokWord, value: string(runes[start:i]), start: start, end: i})
		default:
			tokens = append(tokens, plToken{kind: tokPunct, value: string(ch), start: i, end: i + 1})
			i++
		}
	}
	return tokens
}

// quoted reads a literal opened by quote at runes[open] and returns its
// contents and the position past its closing quote. A doubled quote stands for
// one; with escapes, so does a backslash followed by any character.
func quoted(runes []rune, open int, quote rune, escapes bool) (string, int) {
	var value strings.Builder
	i := open + 1
	for i < len(runes) {
		switch {
		case escapes && runes[i] == '\\' && i+1 < len(runes):
			value.WriteRune(runes[i+1])
			i += 2
		case runes[i] == quote && i+1 < len(runes) && runes[i+1] == quote:
			value.WriteRune(quote)
			i += 2
		case runes[i] == quote:
			return value.String(), i + 1
		default:
			value.WriteRune(runes[i])
			i++
		}
	}
	return value.String(), len(runes)
}

func isPLWordRune(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r >= 0x80
}
