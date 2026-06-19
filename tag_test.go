package main

import (
	"reflect"
	"testing"
)

func TestParseExtraTags(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    map[string]string
		wantErr bool
	}{
		{name: "empty is ok", in: "", want: map[string]string{}},
		{name: "single tag", in: "host=web1", want: map[string]string{"host": "web1"}},
		{name: "multiple tags", in: "host=web1 env=prod", want: map[string]string{"host": "web1", "env": "prod"}},
		{name: "spaces around equals are invalid", in: "host = web1", wantErr: true},
		{name: "missing value", in: "host=", wantErr: true},
		{name: "missing key", in: "=web1", wantErr: true},
		{name: "no separator", in: "host", wantErr: true},
		{name: "double separator", in: "host=a=b", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseExtraTags(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseExtraTags(%q) expected error, got nil", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseExtraTags(%q) unexpected error: %v", tc.in, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseExtraTags(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestAddTags(t *testing.T) {
	// t1 (bucket tags) must override t2 (extra tags); inputs must not be mutated.
	t1 := map[string]string{"host": "web1", "shared": "fromT1"}
	t2 := map[string]string{"env": "prod", "shared": "fromT2"}

	got := addTags(t1, t2)
	want := map[string]string{"host": "web1", "env": "prod", "shared": "fromT1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("addTags = %v, want %v", got, want)
	}

	if t2["shared"] != "fromT2" {
		t.Errorf("addTags mutated its second argument: %v", t2)
	}
}

func TestParseBucketAndTagsSanitizesValues(t *testing.T) {
	clean, tags, err := parseBucketAndTags("cpu.^host=web 1.^zone=a,b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clean != "cpu" {
		t.Errorf("clean bucket = %q, want %q", clean, "cpu")
	}
	if tags["host"] != "web_1" { // space sanitized to _
		t.Errorf("tags[host] = %q, want %q", tags["host"], "web_1")
	}
	if tags["zone"] != "ab" { // comma (delimiter) dropped
		t.Errorf("tags[zone] = %q, want %q", tags["zone"], "ab")
	}
}

func TestNormalizeTags(t *testing.T) {
	tags := map[string]string{"host": "web1", "env": "prod"}
	tests := []struct {
		format uint
		want   string
	}{
		{format: tfPretty, want: "env=prod,host=web1"},
		{format: tfCaret, want: "env=prod.^host=web1"},
		{format: tfGraphite, want: "env.prod.host.web1"},
		{format: tfURI, want: "env=prod&host=web1"},
	}
	for _, tc := range tests {
		if got := normalizeTags(tags, tc.format); got != tc.want {
			t.Errorf("normalizeTags(format=%d) = %q, want %q", tc.format, got, tc.want)
		}
	}
	if got := normalizeTags(map[string]string{}, tfPretty); got != "" {
		t.Errorf("normalizeTags(empty) = %q, want empty", got)
	}
}
