package tsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// Server represents an OpenTSDB server
type Server struct {
	Host string
	Port uint
}

// Get the host:port string for a server
func (s *Server) HostPort() string {
	return fmt.Sprintf("%v:%v", s.Host, s.Port)
}

// TSDB represents an OpenTSDB database serviced by one or more
// Servers.
type TSDB struct {
	Servers []Server
}

// Query takes a TSDB Request and returns the resulting query Response.
func (t *TSDB) Query(req Request) (*Response, error) {
	// TODO: Handle multiple Servers
	host := t.Servers[0].HostPort()
	APIURL := "http://" + host + "/api/query"

	reqJSON, err := json.Marshal(req)

	if err != nil {
		return &Response{}, err
	}

	reqReader := bytes.NewReader(reqJSON)
	respHTTP, err := http.Post(APIURL, "application/json", reqReader)
	if err != nil {
		panic(err)
	}

	respJSON, err := ioutil.ReadAll(respHTTP.Body)
	if err != nil {
		return &Response{}, err
	}

	resp := new(Response)

	err = json.Unmarshal(respJSON, &resp)
	if err != nil {
		return &Response{}, err
	}

	return resp, nil
}

func (t *TSDB) Put(dataPoints []DataPoint) (*PutResponse, error) {
	host := t.Servers[0].HostPort()
	APIURL := "http://" + host + "/api/put?details"

	reqJSON, err := json.Marshal(dataPoints)

	if err != nil {
		return &PutResponse{}, err
	}

	reqReader := bytes.NewReader(reqJSON)

	respHTTP, err := http.Post(APIURL, "application/json", reqReader)
	if err != nil {
		return &PutResponse{}, err
	}

	respJSON, err := ioutil.ReadAll(respHTTP.Body)
	if err != nil {
		return &PutResponse{}, err
	}

	resp := new(PutResponse)
	err = json.Unmarshal(respJSON, &resp)
	if err != nil {
		return &PutResponse{}, err
	}

	return resp, nil
}
