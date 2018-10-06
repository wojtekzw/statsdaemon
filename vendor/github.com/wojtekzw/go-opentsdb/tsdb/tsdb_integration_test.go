// +build integration

// Run integration tests with:
// go test -tags=integration -host=127.0.0.01 -port=4242

package tsdb

import (
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var host = flag.String("host", "127.0.0.1", "host")
var port = flag.Uint("port", 4242, "port")

func TestPut(t *testing.T) {
	assert := assert.New(t)

	db := &TSDB{
		[]Server{
			Server{
				Host: *host,
				Port: *port,
			},
		},
	}

	tags := &Tags{}
	tags.Set("host", "web01")
	tags.Set("dc", "lga")

	metric := &Metric{}
	metric.Set("sys.cpu.nice")

	value := &Value{}
	value.Set(10)

	timestamp := &Time{}
	err := timestamp.Parse(fmt.Sprint(time.Now().Unix()))

	dataPoints := []DataPoint{
		DataPoint{
			Timestamp: timestamp,
			Metric:    metric,
			Value:     value,
			Tags:      tags,
		},
	}

	response, err := db.Put(dataPoints)
	assert.Nil(err)
	assert.NotNil(response)
}

func TestSet(t *testing.T) {
	assert := assert.New(t)
	value := &Value{}

	value.Set(10)
	assert.IsType(int64(10), value.GetInt())
	assert.Equal(int64(10), value.GetInt())

	value.Set(int(10))
	assert.IsType(int64(10), value.GetInt())
	assert.Equal(int64(10), value.GetInt())

	value.Set(int8(10))
	assert.IsType(int64(10), value.GetInt())
	assert.Equal(int64(10), value.GetInt())

	value.Set(int16(10))
	assert.IsType(int64(10), value.GetInt())
	assert.Equal(int64(10), value.GetInt())

	value.Set(int32(10))
	assert.IsType(int64(10), value.GetInt())
	assert.Equal(int64(10), value.GetInt())

	value.Set(int64(10))
	assert.IsType(int64(10), value.GetInt())
	assert.Equal(int64(10), value.GetInt())

	value.Set(float64(10.1))
	assert.IsType(float64(10.1), value.GetFloat())
	assert.Equal(float64(10.1), value.GetFloat())

	value.Set(float32(10.1))
	assert.IsType(float64(10.1), value.GetFloat())
	assert.Equal(float64(float32(10.1)), value.GetFloat())
}
