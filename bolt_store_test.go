package main

import (
	"reflect"
	"testing"

	"github.com/boltdb/bolt"
	"time"
	"os"
)

func TestStoreAndReadMeasurePoint(t *testing.T) {
	type args struct {
		name       string
		mp         MeasurePoint
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantRErr bool
	}{
	// TODO: Add test cases.
		{name: "simple not tags", args:args{name: "one.cout", mp: MeasurePoint{Value: 123, When: 456}}, wantErr: false},
		{name: "complex", args:args{name: "one.^^$$#@@^*((((cout", mp: MeasurePoint{Value: 111, When: 222}}, wantErr: false},
		{name: "empty", args:args{name: "", mp: MeasurePoint{Value: 111, When: 222}}, wantErr: true},
	}
	boltFile := "/tmp/bolt_test.db"
	bucketName := "test_bucket"
	err := os.Remove(boltFile)
	if err != nil {
		// ignore
	}
	dbHandle, err := bolt.Open(boltFile, 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	// dbHandle.NoSync = true

	defer dbHandle.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := storeMeasurePoint(dbHandle,bucketName, tt.args.name, tt.args.mp); (err != nil) != tt.wantErr {
				t.Errorf("storeMeasurePoint() error = %v, wantErr %v", err, tt.wantErr)
			}
			got, err := readMeasurePoint(dbHandle, bucketName, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("readMeasurePoint() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !reflect.DeepEqual(got, tt.args.mp) {
				t.Errorf("readMeasurePoint() = %v, want %v", got, tt.args.mp)
			}

		})
	}
}
