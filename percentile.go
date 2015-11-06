package main

import (
	"fmt"
	"strconv"
	"strings"
)

// Percentiles - slice of percentile with name
type Percentiles []*Percentile

// Percentile - percentile - float with percentile name
type Percentile struct {
	float float64
	str   string
}

// Set percentile structure
func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}
