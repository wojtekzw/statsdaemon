package tsdb

import "testing"

func TestValueSet(t *testing.T) {
	var vf Value
	if err := vf.Set(2.5); err != nil {
		t.Fatalf("Set(float64): %v", err)
	}
	if vf.GetFloat() != 2.5 {
		t.Errorf("GetFloat = %v, want 2.5", vf.GetFloat())
	}

	var vi Value
	if err := vi.Set(7); err != nil {
		t.Fatalf("Set(int): %v", err)
	}
	if vi.GetInt() != 7 {
		t.Errorf("GetInt = %d, want 7", vi.GetInt())
	}

	var vsInt Value
	if err := vsInt.Set("123"); err != nil {
		t.Fatalf("Set(\"123\"): %v", err)
	}
	if vsInt.GetInt() != 123 {
		t.Errorf("GetInt = %d, want 123", vsInt.GetInt())
	}

	var vsFloat Value
	if err := vsFloat.Set("1.5"); err != nil {
		t.Fatalf("Set(\"1.5\"): %v", err)
	}
	if vsFloat.GetFloat() != 1.5 {
		t.Errorf("GetFloat = %v, want 1.5", vsFloat.GetFloat())
	}

	var vBad Value
	if err := vBad.Set([]int{1}); err == nil {
		t.Error("Set(unsupported type) expected error, got nil")
	}
}

func TestMetric(t *testing.T) {
	var m Metric
	if err := m.Set("cpu.load"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if m.Get() != "cpu.load" {
		t.Errorf("Get = %q, want %q", m.Get(), "cpu.load")
	}
}

func TestTags(t *testing.T) {
	var tags Tags
	tags.Set("host", "web1")
	if got := tags.Get("host"); got != "web1" {
		t.Errorf("Get(host) = %q, want web1", got)
	}
	tags.Remove("host")
	if got := tags.Get("host"); got != "" {
		t.Errorf("Get(host) after Remove = %q, want empty", got)
	}
}

func TestTimeParseUnix(t *testing.T) {
	var tm Time
	if err := tm.Parse("1700000000"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tm.Time().Unix() != 1700000000 {
		t.Errorf("Unix = %d, want 1700000000", tm.Time().Unix())
	}

	var bad Time
	if err := bad.Parse("not-a-time"); err == nil {
		t.Error("Parse(invalid) expected error, got nil")
	}
}
