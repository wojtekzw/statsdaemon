package main

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

func graphite(config ConfigApp, deadline time.Time, buffer *bytes.Buffer) error {
	client, err := net.Dial("tcp", config.GraphiteAddress)
	if err != nil {
		return fmt.Errorf("dialing %s failed - %s", config.GraphiteAddress, err)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		return err
	}

	_, err = client.Write(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write stats to graphite: %s", err)
	}

	return nil
}
