package main

import (
	"encoding/json"
	"log"

	"github.com/boltdb/bolt"
)

// MeasurePoint - struct for saving do permanent storage (eg. Bolt)
type MeasurePoint struct {
	Value int64
	When  int64
}

var bucketName = "counters"

func storeMeasurePoint(db *bolt.DB, bucketName string, name string, mp MeasurePoint) error {
	var jsonPoint []byte
	// store some data
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			log.Printf("Error - storeCounter - CreateBucketIfNotExists : %s", err)
			return err
		}
		jsonPoint, err = json.Marshal(mp)
		if err != nil {
			log.Printf("Error - storeCounter - json: %s", err)
			return err
		}
		err = bucket.Put([]byte(name), jsonPoint)
		if err != nil {
			log.Printf("Error - storeCounter - Put: %s", err)
			return err
		}
		return nil
	})

	if err != nil {
		log.Printf("Error - storeCounter: %s", err)
		return err
	}
	return nil
}

func readMeasurePoint(db *bolt.DB, bucketName string, name string) (MeasurePoint, error) {

	outMeasurePoint := MeasurePoint{}
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		// if no bucket - return nil (outMeasurePoint IS empty)
		if bucket == nil {
			return nil
		}

		valBuf := bucket.Get([]byte(name))
		// if no key - return nil (outMeasurePoint IS empty)
		if len(valBuf) == 0 {
			return nil
		}

		err := json.Unmarshal(valBuf, &outMeasurePoint)
		if err != nil {
			log.Printf("Error reading counters: %v", err)
			return err
		}
		return nil
	})

	return outMeasurePoint, err
}
