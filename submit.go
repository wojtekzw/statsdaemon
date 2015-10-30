package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func submit(deadline time.Time, backend string) error {
	var buffer bytes.Buffer
	var num int64
	now := time.Now().Unix()

	// Universal format in buffer
	num += processCounters(&buffer, now, *resetCounters, backend)
	num += processGauges(&buffer, now, backend)
	num += processTimers(&buffer, now, percentThreshold, backend)
	num += processSets(&buffer, now, backend)
	num += processKeyValue(&buffer, now, backend)

	if num == 0 {
		return nil
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: Output line: %s", line)
		}
	}

	// send stats to backend
	switch backend {
	case "external":
		if *postFlushCmd != "stdout" {
			err := sendDataExtCmd(*postFlushCmd, &buffer)
			if err != nil {
				log.Printf(err.Error())
			}
			log.Printf("sent %d stats to external command", num)
		} else {
			if err := sendDataStdout(&buffer); err != nil {
				log.Printf(err.Error())
			}
		}

	case "graphite":
		client, err := net.Dial("tcp", *graphiteAddress)
		if err != nil {
			return fmt.Errorf("dialing %s failed - %s", *graphiteAddress, err)
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
		log.Printf("wrote %d stats to graphite(%s)", num, *graphiteAddress)

	case "opentsdb":
		err := openTSDB(*openTSDBAddress, &buffer, *debug)
		if err != nil {
			log.Printf("Error writing to OpenTSDB: %v\n", err)
		}

	default:
		log.Printf("%v", fmt.Errorf("Invalid backend %s. Exiting...\n", backend))
		os.Exit(1)
	}

	return nil
}
