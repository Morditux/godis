package sortedset

import (
	"testing"
)

// TestParseScoreBordersAreSafe checks border parsing on corner cases,
// especially empty arguments which previously caused an index out of range panic
func TestParseScoreBordersAreSafe(t *testing.T) {
	for _, s := range []string{"", "(", "abc", "1.5", "inf", "+inf", "-inf", "(inf)"} {
		if _, err := ParseScoreBorder(s); err != nil {
			if s == "1.5" || s == "inf" || s == "+inf" || s == "-inf" {
				t.Errorf("ParseScoreBorder(%q) should success, got %v", s, err)
			}
			// invalid arguments must return an error, never panic
		}
	}
	if _, err := ParseScoreBorder("1.5"); err != nil {
		t.Errorf("ParseScoreBorder valid argument should success, got %v", err)
	}
	if _, err := ParseScoreBorder("inf"); err != nil {
		t.Errorf("ParseScoreBorder(inf) should success, got %v", err)
	}
}

func TestParseLexBordersAreSafe(t *testing.T) {
	for _, s := range []string{"", "abc", "[a", "(b", "+", "-"} {
		border, err := ParseLexBorder(s)
		if s == "" || s == "abc" {
			if err == nil {
				t.Errorf("ParseLexBorder(%q) should return error, got %v", s, border)
			}
		} else if err != nil {
			t.Errorf("ParseLexBorder(%q) should success, got %v", s, err)
		}
	}
}
