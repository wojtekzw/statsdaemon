package main

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/boltdb/bolt"
)

var bucketName = "counters"

func storeInt(db *bolt.DB, bucketName string, name string, value int64) error {
	// store some data
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			log.Printf("Error - storeCounter - CreateBucketIfNotExists : %s", err)
			return err
		}

		buf := make([]byte, binary.MaxVarintLen64)
		sizeInt := binary.PutVarint(buf, value)
		err = bucket.Put([]byte(name), buf[:sizeInt])
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

func readInt(db *bolt.DB, bucketName string, name string) (int64, error) {

	var (
		outInt int64
		errInt int
	)

	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %s not found!", bucketName)
		}

		valBuf := bucket.Get([]byte(name))
		outInt, errInt = binary.Varint(valBuf)
		if errInt <= 0 {
			return fmt.Errorf("Error converting to int64. Error code %d", errInt)
		}
		return nil
	})

	return outInt, err
}

// func main() {
// 	db, err := bolt.Open("/tmp/bolt.db", 0644, nil)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	db.NoSync = true
//
// 	defer db.Close()
//
// 	key := "counter"
// 	value := int64(10)
//
// 	for i := 0; i <= 7000; i++ {
// 		value = int64(rand.Intn(10000))
// 		err = storeInt(db, bucketName, key, value)
// 		if err != nil {
// 			log.Printf("Error %s", err)
// 		}
// 		_, err := readInt(db, bucketName, key)
// 		if err != nil {
// 			log.Printf("%s", err)
// 		}
//
// 		// fmt.Printf("In: %d, Out: %d, Equals:%t\n", value, val, value == val)
// 	}
//
// }
