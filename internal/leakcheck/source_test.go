package leakcheck

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// The local copy keeps test binaries standard-library-only. The canonical
// input for this revision is the Git blob below (also available from the
// original tree at 11d25d66db3d:internal/leakcheck/leakcheck.go).
// Regeneration uses a checkout containing this object. Verification works
// directly from an exported source tree.
//
//go:generate go test -timeout=30s -run ^TestPinnedSource$ -args -regenerate
var regenerate = flag.Bool("regenerate", false, "regenerate the shared leakcheck source")

const sourceBlob = "537a7920ef766d14b85e995a7fea260485900356"
const sourceDigest = "9366cd848e4acf9d4bf1246c1a43a2b2ea6311890cca6bac94a5e91feb891b12"
const generatedHeader = "// Code generated from the pinned shared leakcheck source. DO NOT EDIT.\n\n"

func TestPinnedSource(t *testing.T) {
	const dest = "leakcheck.go"
	var data []byte
	var err error
	if *regenerate {
		data, err = exec.CommandContext(t.Context(), "git", "cat-file", "blob", sourceBlob).Output()
	} else {
		data, err = os.ReadFile(dest)
	}
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
	if !*regenerate {
		var ok bool
		data, ok = bytes.CutPrefix(data, []byte(generatedHeader))
		if !ok {
			t.Fatal("missing generated source header")
		}
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != sourceDigest {
		t.Fatalf("shared source digest %s, want %s", got, sourceDigest)
	}
	if *regenerate {
		root, openErr := os.OpenRoot(".")
		if openErr != nil {
			t.Fatal(openErr)
		}
		defer func() { _ = root.Close() }()
		if err = root.WriteFile(dest, append([]byte(generatedHeader), data...), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
