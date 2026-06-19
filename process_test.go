package main

import (
	"bytes"
	"sort"
	"strings"
	"testing"
)

func TestSanitizeBucket(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "abc.def-1_2", want: "abc.def-1_2"},
		{in: "a b", want: "a_b"},
		{in: "a/b", want: "a-b"},
		{in: "a@#$b", want: "ab"},
		{in: "a b/c@d", want: "a_b-cd"},
		{in: "", want: ""},
	}
	for _, tc := range tests {
		if got := sanitizeBucket(tc.in); got != tc.want {
			t.Errorf("sanitizeBucket(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatMetricOutput(t *testing.T) {
	Config.ExtraTagsHash = map[string]string{}
	const now = int64(1700000000)
	tests := []struct {
		name    string
		bucket  string
		value   any
		backend string
		want    string
	}{
		{name: "int64 untagged", bucket: "m", value: int64(5), backend: "external", want: "m 5 1700000000"},
		{name: "int untagged", bucket: "m", value: 5, backend: "external", want: "m 5 1700000000"},
		{name: "float untagged", bucket: "m", value: 2.5, backend: "external", want: "m 2.500000 1700000000"},
		{name: "string untagged", bucket: "m", value: "txt", backend: "external", want: "m txt 1700000000"},
		{name: "tagged external", bucket: "m.^host=web1", value: int64(5), backend: "external", want: "m 5 1700000000 host=web1"},
		{name: "tagged graphite", bucket: "m.^host=web1", value: int64(5), backend: "graphite", want: "m._t_.host.web1 5 1700000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatMetricOutput(tc.bucket, tc.value, now, tc.backend); got != tc.want {
				t.Errorf("formatMetricOutput(%q, %v, %q) = %q, want %q", tc.bucket, tc.value, tc.backend, got, tc.want)
			}
		})
	}
}

func TestProcessKeyValue(t *testing.T) {
	current.keys = make(map[string][]string)
	current.keys["kvbucket"] = []string{"v1", "v2", "v1"} // v1 duplicated, must be emitted once
	now := int64(1700000000)

	var buf bytes.Buffer
	num := current.processKeyValue(&buf, now, "external")

	if num != 1 {
		t.Errorf("processKeyValue num = %d, want 1", num)
	}

	got := splitNonEmpty(buf.String())
	sort.Strings(got)
	want := []string{"kvbucket v1 1700000000", "kvbucket v2 1700000000"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("processKeyValue output = %v, want %v (deduplicated)", got, want)
	}

	if len(current.keys) != 0 {
		t.Errorf("processKeyValue did not purge current.keys: %v", current.keys)
	}
}

func TestProcessGaugesEviction(t *testing.T) {
	Config.DeleteGauges = true
	current.gauges = make(map[string]float64)
	lastGaugeValue = make(map[string]float64)
	now := int64(1700000000)

	var buf bytes.Buffer
	current.gauges["g.evict"] = 5
	current.processGauges(&buf, now, "external") // emit value, set sentinel + lastGaugeValue
	if _, ok := lastGaugeValue["g.evict"]; !ok {
		t.Fatal("lastGaugeValue should be set after first cycle")
	}

	// Second cycle with no new value: delete-gauges mode must evict the bucket
	// from both maps so they do not grow unbounded.
	current.processGauges(&buf, now, "external")
	if len(current.gauges) != 0 {
		t.Errorf("current.gauges not evicted: %v", current.gauges)
	}
	if len(lastGaugeValue) != 0 {
		t.Errorf("lastGaugeValue not evicted: %v", lastGaugeValue)
	}
}

func TestPrefixPresent(t *testing.T) {
	patterns := []string{"app.", "sys.cpu"}
	tests := []struct {
		in   string
		want bool
	}{
		{in: "app.requests", want: true},
		{in: "sys.cpu.load", want: true},
		{in: "other.metric", want: false},
		{in: "ap", want: false},
	}
	for _, tc := range tests {
		if got := prefixPresent(tc.in, patterns); got != tc.want {
			t.Errorf("prefixPresent(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// splitNonEmpty splits s on newlines and drops empty lines.
func splitNonEmpty(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}
