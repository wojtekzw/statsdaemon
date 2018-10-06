// http://opentsdb.net/docs/build/html/api_http/serializers/json.html#api-query

package tsdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Request represents the information needed to query a TSDB for timeseries data.
type Request struct {
	Start   *Time   `json:"start"`
	End     *Time   `json:"end,omitempty"`     // Optional
	Padding bool    `json:"padding,omitempty"` // Optional
	Queries []Query `json:"queries"`
}

/*
Time represents a timeseries time value.

Valid formats for Time are:
	Relative (see: http://opentsdb.net/docs/build/html/user_guide/query/index.html#relative)
	Unix     (see: http://opentsdb.net/docs/build/html/user_guide/query/index.html#absolute-unix-time)
	Absolute (see: http://opentsdb.net/docs/build/html/user_guide/query/index.html#absolute-formatted-time)
*/
type Time struct {
	time   time.Time
	format string
	string
}

func (t *Time) Time() time.Time {
	return t.time
}

// UnmarshalJSON implements json.Unmarshaler for consistant conversion from JSON.
func (t *Time) UnmarshalJSON(inJSON []byte) error {
	var raw interface{}
	err := json.Unmarshal(inJSON, &raw)
	if err != nil { panic(err) }

	switch raw.(type) {
	case float64:
		err = t.Parse(strconv.FormatInt(int64(raw.(float64)), 10))
	case string:
		err = t.Parse(raw.(string))
	}
	return err
}

// MarshalJSON implements json.Marshaler for consistant conversion to JSON.
func (t *Time) MarshalJSON() ([]byte, error) {
	switch t.format {
	case "":         return nil, nil
	case "Unix":     return json.Marshal(t.time.Unix())
	case "Absolute": return json.Marshal(t.AbsoluteTime())
	case "Relative": return json.Marshal(t.string)
	default:         return json.Marshal(t.time.Unix())
	}
}

/*
Parse takes a string, verifies that it is a valid tsdb.Time, and if so sets t to that time
and returns a nil error.

If the input string is not a valid Time then t is unchanged and err contains the error.
*/
func (t *Time) Parse(timeIn string) error {
	switch {
	case isAbsoluteTime(timeIn): return t.fromAbsoluteTime(timeIn)
	case isRelativeTime(timeIn): return t.fromRelativeTime(timeIn)
	case isUnixTime(timeIn):     return t.fromUnixTime(timeIn)
	default:                     return fmt.Errorf("Invalid Time Value")
	}
	return fmt.Errorf("Invalid Time Value (Uncaught)")
}

// isValidTime verifies that a string can be converted to a Time.
func isValidTime(timeIn string) bool {
	if isAbsoluteTime(timeIn) || isRelativeTime(timeIn) || isUnixTime(timeIn) {
		return true
	}
	return false
}

/*
isAbsoluteTime verifies if a string is a valid Absolute format time.
Valid formats are:
	yyyy/MM/dd-HH:mm:ss
	yyyy/MM/dd HH:mm:ss
	yyyy/MM/dd-HH:mm
	yyyy/MM/dd HH:mm
	yyyy/MM/dd
*/
func isAbsoluteTime(timeIn string) bool {
	pattern := `^\d{4}\/\d{1,2}\/\d{1,2}`
	match, err := regexp.MatchString(pattern, timeIn)
	if err != nil { panic(err) }
	return match
}

/*
isAbsoluteTime verifies if a string is a valid Relative format time.

Valid formats are:
	[0-9]*{s,m,h,d,w,n,y}-ago
*/
func isRelativeTime(timeIn string) bool {
	pattern := `^\d+[smhdwmny]\-ago`
	match, err := regexp.MatchString(pattern, timeIn)
	if err != nil { panic(err) }
	return match
}

/*
isUnixTime verifies if a string is a valid Unix format time.

Valid formats are:
	10-digit integer
	13-digit optional millisecond precision
*/
func isUnixTime(timeIn string) bool {
	pattern := `^\d{10}|^\d{13}`
	match, err := regexp.MatchString(pattern, timeIn)
	if err != nil { panic(err) }
	return match
}

// fromAbsoluteTime parses the provided timeIn string and if possible
// assigns the time to Time t.
func (t *Time) fromAbsoluteTime(timeIn string) (err error) {
	t.format = "Absolute"
	t.string = timeIn
	t.time, err = time.Parse("2006/01/02-15:04:05", timeIn)
	if err == nil { return }
	t.time, err = time.Parse("2006/01/02 15:04:05", timeIn)
	if err == nil { return }
	t.time, err = time.Parse("2006/01/02-15:04", timeIn)
	if err == nil { return }
	t.time, err = time.Parse("2006/01/02 15:04", timeIn)
	if err == nil { return }
	t.time, err = time.Parse("2006/01/02-15", timeIn)
	if err == nil { return }
	t.time, err = time.Parse("2006/01/02 15", timeIn)
	if err == nil { return }
	t.time, err = time.Parse("2006/01/02", timeIn)
	return
}

// AbsoluteTime returns the string version of a Time in Absolute format.
func (t *Time) AbsoluteTime() (string) {
	switch {
	case t.time.Second() > 0: return t.time.Format("2006/01/02-15:04:05")
	case t.time.Minute() > 0: return t.time.Format("2006/01/02-15:04")
	case t.time.Hour()   > 0: return t.time.Format("2006/01/02-15")
	}
	return t.time.Format("2006/01/02")
}

// fromRelativeTime parses the provided timeIn string and if possible
// assigns the time to Time t.
func (t *Time) fromRelativeTime(timeIn string) error {
	t.format = "Relative"
	t.string = timeIn
	return nil
}

// RelativeTime returns the string version of a Time in Relative format.
func (t *Time) RelativeTime() (string) {
	return t.time.Format("2006/01/02-15:04:05")
}

// fromUnixTime parses the provided timeIn string and if possible
// assigns the time to Time t.
func (t *Time) fromUnixTime(timeIn string) (err error) {
	t.format = "Unix"
	t.string = timeIn
	var timeInInt64 int64
	timeInInt64, err = strconv.ParseInt(timeIn, 10, 64)
	if err != nil { return err }
	t.time = time.Unix(timeInInt64, 0)
	return nil
}

/*
Response respresents a Response from an OpenTSDB query 
made up of zero or more result types.

See: http://opentsdb.net/docs/build/html/api_http/serializers/json.html#Response
*/
type Response []result

/*
result is a single timeseries or aggregate Response from an OpenTSDB query.

See: http://opentsdb.net/docs/build/html/api_http/serializers/json.html#Response
*/
type result struct {
	Metric         Metric            `json:"metric"`
	Tags           map[string]string `json:"tags"`
	Dps            Dps               `json:"dps"`
	AggregatedTags []string          `json:"aggregateTags"`
}

// Query represents the information needed for a single query to OpenTSDB sans
// time interval.  A Query can be added to a TSDB Request.
type Query struct {
	Aggregator string            `json:"aggregator"`
	Metric     Metric            `json:"metric"`
	Rate       bool              `json:"rate"`
	Downsample string            `json:"downsample,omitempty"`
	Tags       map[string]string `json:"tags"`
}
