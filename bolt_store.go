package main

import (
	"encoding/json"
	"errors"
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

	if err := checkNames(bucketName, name); err != nil {
		return err
	}
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		jsonPoint, err = json.Marshal(mp)
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(name), jsonPoint)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func readMeasurePoint(db *bolt.DB, bucketName string, name string) (MeasurePoint, error) {

	outMeasurePoint := MeasurePoint{}
	if err := checkNames(bucketName, name); err != nil {
		return outMeasurePoint, err
	}
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		// if no bucket - return nil (outMeasurePoint is empty)
		if bucket == nil {
			return nil
		}

		valBuf := bucket.Get([]byte(name))
		// if no key - return nil (outMeasurePoint is empty)
		if len(valBuf) == 0 {
			return nil
		}

		err := json.Unmarshal(valBuf, &outMeasurePoint)
		if err != nil {
			return err
		}
		return nil
	})

	return outMeasurePoint, err
}

func checkNames(bucketName string, name string) error {

	if len(bucketName) == 0 {
		return errors.New("bucket name can't be empty")
	}
	if len(name) == 0 {
		return errors.New("key name can't be empty")
	}
	return nil
}
