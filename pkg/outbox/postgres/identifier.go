package postgres

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// quoteTableName parses a table name of the form `table` or `schema.table` and
// returns it as a safely quoted SQL identifier.
//
// Each component is either unquoted (letters, digits, '_' and '$', not starting
// with a digit; folded to lower case like PostgreSQL does) or double-quoted
// (any characters except NUL, with `""` as an escaped quote; case preserved).
// Anything else is rejected.
func quoteTableName(name string) (string, error) {
	var parts pgx.Identifier
	rest := name
	for {
		part, remaining, err := parseIdentifierPart(rest)
		if err != nil {
			return "", fmt.Errorf("invalid table name %q: %w", name, err)
		}
		parts = append(parts, part)
		if remaining == "" {
			break
		}
		if remaining[0] != '.' {
			return "", fmt.Errorf("invalid table name %q: unexpected character %q", name, remaining[0])
		}
		rest = remaining[1:]
		if len(parts) == 2 {
			return "", fmt.Errorf("invalid table name %q: expected at most schema.table", name)
		}
	}
	return parts.Sanitize(), nil
}

func parseIdentifierPart(s string) (part, rest string, err error) {
	if s == "" {
		return "", "", fmt.Errorf("empty identifier")
	}
	if s[0] == '"' {
		var b strings.Builder
		for i := 1; i < len(s); i++ {
			if s[i] != '"' {
				b.WriteByte(s[i])
				continue
			}
			if i+1 < len(s) && s[i+1] == '"' {
				b.WriteByte('"')
				i++
				continue
			}
			if b.Len() == 0 {
				return "", "", fmt.Errorf("empty quoted identifier")
			}
			if strings.ContainsRune(b.String(), 0) {
				return "", "", fmt.Errorf("identifier contains NUL")
			}
			return b.String(), s[i+1:], nil
		}
		return "", "", fmt.Errorf("unterminated quoted identifier")
	}
	end := 0
	for end < len(s) && s[end] != '.' {
		c := s[end]
		isLetter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		if !isLetter && !(end > 0 && (isDigit || c == '$')) {
			return "", "", fmt.Errorf("invalid character %q (quote the identifier to use it)", c)
		}
		end++
	}
	if end == 0 {
		return "", "", fmt.Errorf("empty identifier")
	}
	return strings.ToLower(s[:end]), s[end:], nil
}
