package tsdb

type PutResponse struct {
	Failed  int        `json"failed"`
	Success int        `json"success"`
	Errors  []PutError `json"errors"`
}

type PutError struct {
	DataPoint *DataPoint `json"datapoint"`
	Error     string     `json"error"`
}
