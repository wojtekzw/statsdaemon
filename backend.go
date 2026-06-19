package main

import (
	"bytes"
	"fmt"
	"os"
	"time"
)

// Backend transmits a serialized batch of metrics to a destination.
// deadline is a hint for network backends; other backends may ignore it.
type Backend interface {
	Send(buf *bytes.Buffer, deadline time.Time) error
}

// stdoutBackend writes the batch to stdout (external backend, no post-flush cmd).
type stdoutBackend struct{}

func (stdoutBackend) Send(buf *bytes.Buffer, _ time.Time) error { return sendDataStdout(buf) }

// extCmdBackend pipes the batch to an external command's stdin.
type extCmdBackend struct{ cmd []string }

func (b extCmdBackend) Send(buf *bytes.Buffer, _ time.Time) error {
	return sendDataExtCmd(b.cmd, buf)
}

type graphiteBackend struct{ cfg ConfigApp }

func (b graphiteBackend) Send(buf *bytes.Buffer, deadline time.Time) error {
	return graphite(b.cfg, deadline, buf)
}

type opentsdbBackend struct{ cfg ConfigApp }

func (b opentsdbBackend) Send(buf *bytes.Buffer, _ time.Time) error {
	return openTSDB(b.cfg, buf)
}

type fileBackend struct{ f *os.File }

func (b fileBackend) Send(buf *bytes.Buffer, _ time.Time) error {
	return sendDataToFile(b.f, buf)
}

// selectBackend returns the Backend for the configured backend-type.
// A nil Backend with a nil error means the no-op ("dummy") backend.
func selectBackend(cfg ConfigApp) (Backend, error) {
	switch cfg.BackendType {
	case "external":
		if cfg.PostFlushCmd != "stdout" {
			return extCmdBackend{cmd: cfg.ParsedPostFlushCmd}, nil
		}
		return stdoutBackend{}, nil
	case "graphite":
		return graphiteBackend{cfg: cfg}, nil
	case "opentsdb":
		return opentsdbBackend{cfg: cfg}, nil
	case "file":
		return fileBackend{f: cfg.CfgFileBackend.LogFile}, nil
	case "dummy":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid backend `%s`", cfg.BackendType)
	}
}
