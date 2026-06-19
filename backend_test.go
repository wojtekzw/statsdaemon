package main

import (
	"fmt"
	"testing"
)

func TestSelectBackend(t *testing.T) {
	tests := []struct {
		name     string
		cfg      ConfigApp
		wantType string // %T of the returned backend, "<nil>" for dummy
		wantErr  bool
	}{
		{name: "stdout", cfg: ConfigApp{BackendType: "external", PostFlushCmd: "stdout"}, wantType: "main.stdoutBackend"},
		{name: "extcmd", cfg: ConfigApp{BackendType: "external", PostFlushCmd: "/bin/cat"}, wantType: "main.extCmdBackend"},
		{name: "graphite", cfg: ConfigApp{BackendType: "graphite"}, wantType: "main.graphiteBackend"},
		{name: "opentsdb", cfg: ConfigApp{BackendType: "opentsdb"}, wantType: "main.opentsdbBackend"},
		{name: "file", cfg: ConfigApp{BackendType: "file"}, wantType: "main.fileBackend"},
		{name: "dummy", cfg: ConfigApp{BackendType: "dummy"}, wantType: "<nil>"},
		{name: "invalid", cfg: ConfigApp{BackendType: "nope"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := selectBackend(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("selectBackend(%q) expected error", tc.cfg.BackendType)
				}
				return
			}
			if err != nil {
				t.Fatalf("selectBackend(%q) unexpected error: %v", tc.cfg.BackendType, err)
			}
			if got := fmt.Sprintf("%T", b); got != tc.wantType {
				t.Errorf("selectBackend(%q) type = %s, want %s", tc.cfg.BackendType, got, tc.wantType)
			}
		})
	}
}
