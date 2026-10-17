package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"appendonly yes\n" +
		"peers a,b"
	p, err := parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if p.Bind != "0.0.0.0" {
		t.Error("string parse failed")
	}
	if p.Port != 6399 {
		t.Error("int parse failed")
	}
	if !p.AppendOnly {
		t.Error("bool parse failed")
	}
}

func TestParseKeepsDefaults(t *testing.T) {
	p, err := parse(strings.NewReader("appendonly no\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p.Bind != "127.0.0.1" {
		t.Fatalf("bind default lost: %q", p.Bind)
	}
	if p.Port != 6379 {
		t.Fatalf("port default lost: %d", p.Port)
	}
	if p.Databases != 16 {
		t.Fatalf("databases default lost: %d", p.Databases)
	}
	if p.AppendFilename != "appendonly.aof" {
		t.Fatalf("appendfilename default lost: %q", p.AppendFilename)
	}
}

func TestParseHandlesWhitespaceAndComments(t *testing.T) {
	src := "\n" +
		"   \n" +
		"\t\t\n" +
		"  # comment with leading spaces\n" +
		"bind\t127.0.0.1\n" +
		"port 6399\n"
	p, err := parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if p.Bind != "127.0.0.1" || p.Port != 6399 {
		t.Fatalf("unexpected parse result: bind=%q port=%d", p.Bind, p.Port)
	}
}

func TestParseBoolVariants(t *testing.T) {
	trueValues := []string{"yes", "YES", "true", "1", "on"}
	for _, v := range trueValues {
		p, err := parse(strings.NewReader("appendonly " + v + "\n"))
		if err != nil {
			t.Fatalf("parse %q: %v", v, err)
		}
		if !p.AppendOnly {
			t.Errorf("appendonly %q should parse as true", v)
		}
	}
	falseValues := []string{"no", "NO", "false", "0", "off"}
	for _, v := range falseValues {
		p, err := parse(strings.NewReader("appendonly " + v + "\n"))
		if err != nil {
			t.Fatalf("parse %q: %v", v, err)
		}
		if p.AppendOnly {
			t.Errorf("appendonly %q should parse as false", v)
		}
	}
	// an invalid boolean keeps the default instead of silently becoming false
	p, err := parse(strings.NewReader("appendonly maybe\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p.AppendOnly {
		t.Fatal("invalid boolean should keep the default")
	}
}

func TestParseRejectsInvalidValues(t *testing.T) {
	cases := []string{
		"port 0\n",
		"port 70000\n",
		"databases 0\n",
		"maxclients -1\n",
		"appendfsync everysek\n",
	}
	for _, src := range cases {
		if _, err := parse(strings.NewReader(src)); err == nil {
			t.Errorf("parse(%q) should have failed", src)
		}
	}
}

func TestParseUnknownKeyIgnored(t *testing.T) {
	p, err := parse(strings.NewReader("unknown-key value\nport 6399\n"))
	if err != nil {
		t.Fatalf("unknown keys must not prevent startup: %v", err)
	}
	if p.Port != 6399 {
		t.Fatalf("port not parsed: %d", p.Port)
	}
}

func TestPathHelpers(t *testing.T) {
	p := defaultProperties()
	p.Dir = "data"
	p.AppendFilename = "appendonly.aof"
	if got, want := p.AofPath(), filepath.Join("data", "appendonly.aof"); got != want {
		t.Fatalf("AofPath = %q, want %q", got, want)
	}
	p.RDBFilename = "dump.rdb"
	if got, want := p.RdbPath(), filepath.Join("data", "dump.rdb"); got != want {
		t.Fatalf("RdbPath = %q, want %q", got, want)
	}
	if got, want := p.DumpPath(), filepath.Join("data", "dump.rdb"); got != want {
		t.Fatalf("DumpPath = %q, want %q", got, want)
	}
	abs := filepath.Join(string(filepath.Separator), "tmp", "abs.aof")
	p.AppendFilename = abs
	if got := p.AofPath(); got != abs {
		t.Fatalf("absolute AofPath = %q, want %q", got, abs)
	}
}

func TestSetupConfig(t *testing.T) {
	oldProperties := Properties
	oldConfigPath := configFilePath
	defer func() {
		Properties = oldProperties
		configFilePath = oldConfigPath
	}()

	dir := t.TempDir()
	path := filepath.Join(dir, "godis.conf")
	content := "port 16400\nappendonly yes\ndir data\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SetupConfig(path); err != nil {
		t.Fatalf("SetupConfig failed: %v", err)
	}
	if Properties.Port != 16400 {
		t.Fatalf("port = %d", Properties.Port)
	}
	if Properties.Bind != "127.0.0.1" {
		t.Fatalf("bind default lost: %q", Properties.Bind)
	}
	if Properties.AppendFilename != "appendonly.aof" {
		t.Fatalf("appendfilename default missing: %q", Properties.AppendFilename)
	}
	if got, want := Properties.AofPath(), filepath.Join("data", "appendonly.aof"); got != want {
		t.Fatalf("AofPath = %q, want %q", got, want)
	}
	if configFilePath != path {
		t.Fatalf("configFilePath = %q, want %q", configFilePath, path)
	}
}

func TestSetupConfigMissingFile(t *testing.T) {
	oldProperties := Properties
	defer func() { Properties = oldProperties }()
	if err := SetupConfig(filepath.Join(t.TempDir(), "does-not-exist.conf")); err == nil {
		t.Fatal("SetupConfig must return an error for a missing file")
	}
}
