package tsdb

import (
	"bytes"
	"encoding/json"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"strings"
	"testing"

	// "github.com/davecgh/go-spew/spew"
)

// Hook up gocheck into the gotest runner
func Test(t *testing.T) { TestingT(t) }

type tsdbSuite struct {
	db    *TSDB
	reqs  []Request
	resps []Response
	// dpts      []dataPoint
	// queries   []query
	reqsJSON     [][]byte // Correct JSON requests. Populated by json files.
	errReqsJSON  [][]byte // Bad JSON requests.
	respsJSON    [][]byte // Correct JSON responses. Populated by json files.
	errRespsJSON [][]byte // Bad JSON requests.
}

// Tie our test suite into gocheck
var _ = Suite(&tsdbSuite{})

// func (s *tsdbSuite) Test00ToFromJson(c *C) {
// 	for i, v := range s.reqsJSON {
// 		testJSON, err := compactJSON(v)
// 		c.Assert(err, IsNil)
// 		fromJSON := new(Request)
// 		err = json.Unmarshal(testJSON, &fromJSON)
// 		c.Assert(err, IsNil)
// 		s.reqs = append(s.reqs, *fromJSON)
// 		JSONFromReq, err := json.Marshal(s.reqs[i])
// 		c.Assert(err, IsNil)
// 		var raw0, raw1 interface{}
// 		err = json.Unmarshal(JSONFromReq, &raw0)
// 		c.Assert(err, IsNil)
// 		err = json.Unmarshal(testJSON, &raw1)
// 		c.Assert(err, IsNil)
// 		c.Assert(raw0, DeepEquals, raw1)
// 	}
// }

// func (s *tsdbSuite) Test01Query(c *C) {
// 	for i, v := range s.reqs {
// 		qResp, err := s.db.Query(v)
// 		c.Assert(err, IsNil)
// 		var JSONFromResp []byte
// 		JSONFromResp, err = json.Marshal(qResp)
// 		c.Assert(err, IsNil)
// 		var raw0, raw1 interface{}
// 		err = json.Unmarshal(JSONFromResp, &raw0)
// 		c.Assert(err, IsNil)
// 		err = json.Unmarshal(s.respsJSON[i], &raw1)
// 		c.Assert(err, IsNil)
// 		c.Assert(raw0, DeepEquals, raw1)
// 		s.resps = append(s.resps, *qResp)
// 	}
// }

func (s *tsdbSuite) SetUpSuite(c *C) {
	var err error

	// Connect to a TSDB server to test against
	// s.conn, err = Dial("testtsdb", 4242)
	// if err != nil { panic(err) }

	testserver := &Server{Host: "192.168.59.103", Port: 4242}
	s.db = &TSDB{Servers: []Server{*testserver}}

	// Load from JSON files
	testFiles, err := ioutil.ReadDir("test-metrics/json/")
	if err != nil {
		panic(err)
	}
	for _, v := range testFiles {
		if v.IsDir() {
			continue
		}
		if strings.Contains(v.Name(), "-err-request.json") {
			json, err := ioutil.ReadFile("test-metrics/json/" + v.Name())
			if err != nil {
				panic(err)
			}
			s.errReqsJSON = append(s.errReqsJSON, json)
			continue
		}
		if strings.Contains(v.Name(), "-err-response.json") {
			json, err := ioutil.ReadFile("test-metrics/json/" + v.Name())
			if err != nil {
				panic(err)
			}
			s.errRespsJSON = append(s.errRespsJSON, json)
			continue
		}
		if strings.Contains(v.Name(), "-request.json") {
			json, err := ioutil.ReadFile("test-metrics/json/" + v.Name())
			if err != nil {
				panic(err)
			}
			s.reqsJSON = append(s.reqsJSON, json)
			continue
		}
		if strings.Contains(v.Name(), "-response.json") {
			json, err := ioutil.ReadFile("test-metrics/json/" + v.Name())
			if err != nil {
				panic(err)
			}
			s.respsJSON = append(s.respsJSON, json)
		}
	}
}

// Helper functions
func compactJSON(inJSON []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := json.Compact(&buf, inJSON)
	if err != nil {
		return []byte(nil), err
	}
	return buf.Bytes(), nil
}
