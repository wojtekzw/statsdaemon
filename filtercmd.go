// filtercmd.go
package main

import (
	"log"
	"os/exec"
	"os"
	"bytes"
)

// If we don't have any command to call after each flush, we simply write
// data on Stdout.
func sendDataStdout(buf *bytes.Buffer) error {
	if _, err := os.Stdout.Write(buf.Bytes()) ; err != nil {
		return err
	}
	return nil
}

// If we do have a command to run on each flush, run this command and pipe
// to it data via Stdin.
func sendDataExtCmd(filtercmd string, buf *bytes.Buffer) error {
	// Setup a command structure and connect necessary
	// plumbing to make sure command receives stdin from caller
	// and stdout is connected to system's stdout, so that system
	// actually sees output instead of output being returned to caller
	// and likely ignored completely.
	cmd := exec.Command(filtercmd)
	stdin, err := cmd.StdinPipe() // allow caller to send data to command
	cmd.Stdout = os.Stdout // we don't want output of command coming back
	if err != nil {
		return err
	}

	log.Printf("Inside sendDataExtCmd function\n")
	if _, err := stdin.Write(buf.Bytes()) ; err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	if err := stdin.Close() ; err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}
