package task

import (
	"regexp"
	"testing"
)

func TestGenerateID_Format(t *testing.T) {
	id := GenerateID()
	if matched, _ := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id); !matched {
		t.Fatalf("ID %q does not match UUIDv4 format", id)
	}
}

func TestGenerateID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := GenerateID()
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		seen[id] = struct{}{}
	}
}

func TestGenerateID_VersionAndVariant(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := GenerateID()
		// 15th hex char (14 in 0-indexed) must be '4' (UUIDv4)
		if id[14] != '4' {
			t.Fatalf("expected version 4, got %q at pos 14 (id=%s)", id[14], id)
		}
		// 20th hex char (19 in 0-indexed) must be 8/9/a/b (variant 10xx)
		c := id[19]
		if !(c == '8' || c == '9' || c == 'a' || c == 'b') {
			t.Fatalf("expected variant 8/9/a/b, got %q (id=%s)", c, id)
		}
	}
}
