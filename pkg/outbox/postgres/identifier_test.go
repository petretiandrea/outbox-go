package postgres

import "testing"

func TestQuoteTableName(t *testing.T) {
	ok := map[string]string{
		"outbox_messages":  `"outbox_messages"`,
		"Outbox":           `"outbox"`,
		"public.outbox":    `"public"."outbox"`,
		`"MyTable"`:        `"MyTable"`,
		`"my schema"."T"`:  `"my schema"."T"`,
		`"we""ird"`:        `"we""ird"`,
		`"a.b"`:            `"a.b"`,
	}
	for in, want := range ok {
		got, err := quoteTableName(in)
		if err != nil || got != want {
			t.Errorf("quoteTableName(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	bad := []string{`a;drop table x`, `a b`, `a.b.c`, `a.`, `.a`, `"a`, `""`, `1abc`, `a"b`, `"a"b`, "a\x00", `"a` + "\x00" + `"`, `a--`}
	for _, in := range bad {
		if got, err := quoteTableName(in); err == nil {
			t.Errorf("quoteTableName(%q) = %q; want error", in, got)
		}
	}
}
