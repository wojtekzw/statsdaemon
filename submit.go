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

	num += processCounters(&buffer, now, *resetCounters)
	num += processGauges(&buffer, now)
	num += processTimers(&buffer, now, percentThreshold)
	num += processSets(&buffer, now)
	num += processKeyValue(&buffer, now)

	if num == 0 {
		return nil
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: %s", line)
		}
	}
	// send stats to backend
	switch backend {
	case "external":
		log.Printf("DEBUG: external [%s]\n", fixNewLine(buffer.String()))

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
		log.Printf("DEBUG: graphite [%s]\n", fixNewLine(buffer.String()))

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
		if *debug {
			log.Printf("DEBUG: opentsdb [%s]\n", fixNewLine(buffer.String()))
		}

		err := openTSDB(*openTSDBAddress, &buffer, tags, *debug)
		if err != nil {
			log.Printf("Error writing to OpenTSDB: %v\n", err)
		}

	default:
		log.Printf("%v", fmt.Errorf("Invalid backend %s. Exiting...\n", backend))
		os.Exit(1)
	}

	return nil
}
