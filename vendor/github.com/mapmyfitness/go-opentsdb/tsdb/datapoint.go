package tsdb

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

/*
DataPoint represents a single data point good for storing in OpenTSDB.

See: http://opentsdb.net/docs/build/html/api_http/serializers/json.html#api-put
*/

type DataPoint struct {
	Timestamp *Time   `json:"timestamp"`
	Metric    *Metric `json:"metric"`
	Value     *Value  `json:"value"`
	Tags      *Tags   `json:"tags,omitempty"`
}

type Dps struct {
	DataPoints []DataPoint
}

func (d *Dps) UnmarshalJSON(inJSON []byte) error {
	var dps map[string]interface{}
	json.Unmarshal(inJSON, &dps)

	// Sort by key (timestamp) since a map isn't sorted
	var keys []string
	for k := range dps {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create ordered list of points
	d.DataPoints = make([]DataPoint, len(keys))
	for index, key := range keys {
		time := &Time{}
		time.Parse(key)

		value := &Value{}
		value.Set(dps[key])

		dataPoint := DataPoint{
			Timestamp: time,
			Value:     value,
		}
		d.DataPoints[index] = dataPoint
	}

	return nil
}

// Timestamp represents a Unix timestamp.
type Timestamp struct {
	time.Time
}

// Get retrieves the Unix style time of a Timestamp.
func (t Timestamp) Get() int64 {
	return t.Unix()
}

// Set a Timestamp from a provided Unix time.
func (t Timestamp) Set(inTime int64) error {
	// TODO: Support milliseconds
	// TODO: Sanity check for absurd time value?
	t.Time = time.Unix(inTime, 0)
	return nil
}

// Metric stores the name of an OpenTSDB metric.
type Metric struct {
	string
}

// Get retrieves a Metric's string value.
func (m *Metric) Get() string {
	return m.string
}

// Set a Metric's string value.
func (m *Metric) Set(name string) error {
	// TODO: Sanity check for invalid characters
	m.string = name
	return nil
}

func (m *Metric) UnmarshalJSON(inJSON []byte) error {
	json.Unmarshal(inJSON, &m.string)
	return nil
}

func (m *Metric) MarshalJSON() ([]byte, error) {
	// TODO: Check for empty metric?
	return json.Marshal(m.string)
}

// Value stores a single timeseries data value.
type Value struct {
	float64 float64
	int64   int64
}

// Get a Value's float64 representation
func (v *Value) GetFloat() float64 {
	return v.float64
}

// Get a Value's int representation
func (v *Value) GetInt() int64 {
	return v.int64
}

/*
Set a Value

The following types are accepted:
	tsdb.Value
	float64
	string
	int
*/
func (v *Value) Set(quantity interface{}) error {
	switch quantity.(type) {
	default:
		return errors.New("Invalid Value")
	case Value:
		v.float64 = quantity.(float64)
		v.int64 = quantity.(int64)
		return nil
	case float64:
		v.float64 = quantity.(float64)
		return nil
	case float32:
		v.float64 = float64(quantity.(float32))
		return nil
	case string:
		stringv := quantity.(string)
		if strings.Contains(stringv, ".") {
			floatv, err := strconv.ParseFloat(quantity.(string), 64)
			if err != nil {
				return err
			}
			v.float64 = floatv
		} else {
			intv, err := strconv.ParseInt(quantity.(string), 10, 64)
			if err != nil {
				return err
			}
			v.int64 = int64(intv)
		}
		return nil
	case int:
		v.int64 = int64(quantity.(int))
		return nil
	case int64:
		v.int64 = int64(quantity.(int64))
		return nil
	case int32:
		v.int64 = int64(quantity.(int32))
		return nil
	case int16:
		v.int64 = int64(quantity.(int16))
		return nil
	case int8:
		v.int64 = int64(quantity.(int8))
		return nil
	}
}

func (v *Value) UnmarshalJSON(inJSON []byte) error {
	v.Set(inJSON)
	return nil
}

func (v *Value) MarshalJSON() ([]byte, error) {
	if v.float64 != 0 {
		return json.Marshal(v.float64)
	} else if v.int64 != 0 {
		return json.Marshal(v.int64)
	} else {
		return json.Marshal(0)
	}
	return json.Marshal(v.float64)
}

// Tags contains key/value pairs representing tags
type Tags struct {
	tags map[string]string
}

// Get the value of a tag matching a provided key
func (t *Tags) Get(key string) string {
	return t.tags[key]
}

// Set updates an existing tag or creates a tag if the provided key
// is yet in use.
func (t *Tags) Set(key, value string) error {
	if t.tags == nil {
		t.tags = make(map[string]string)
	}

	// TODO: Sanity check for invalid characters
	t.tags[key] = value
	return nil
}

// Remove a tag with provided key from a collection of Tags
func (t *Tags) Remove(key string) {
	delete(t.tags, key)
}

func (t *Tags) MarshalJSON() ([]byte, error) {
	// TODO: Check for empty metric?
	return json.Marshal(t.tags)
}
