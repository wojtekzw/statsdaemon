package main

import (
	"fmt"
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
)

// tag form types
const (
	tfCaret    = iota // eg. cpu.load.^host=h1.^env=dev
	tfGraphite = iota // eg. cpu.load._t_.host.h1.env.dev
	tfURI      = iota //eg. cpu.load?host=h1&env=dev
	tfPretty   = iota //eg. cpu.load host=h1,env=dev
)

var (
	tfDefault uint = tfCaret
)

// delimiter between bucket name and tags
const (
	tfCaretFirstDelim    = ".^"
	tfGraphiteFirstDelim = "._t_."
	tfURIFirstDelim      = "?"
	tfPrettyFirstDelim   = ""
)

// delimeite between tag(key) and value
const (
	tfCaretKVDelim    = "="
	tfGraphiteKVDelim = "."
	tfURIKVDelim      = "="
	tfPrettyKVDelim   = "="
)

// delimiters between tags for different tag formats
const (
	tfCaretTagsDelim    = ".^"
	tfGraphiteTagsDelim = "."
	tfURITagsDelim      = "&"
	tfPrettyTagsDelim   = ","
)

func tagsDelims(tf uint) (string, string, string) {
	var (
		firstDelim, kvDelim, tagsDelim string
	)

	// checking tf in default in switch is too late
	// doing it now

	if tf != tfCaret && tf != tfGraphite && tf != tfURI && tf != tfPretty {
		log.Printf("Error - tagsDelims: Unknown tag format %d. Setting to default = %d", tf, tfDefault)
		tf = tfDefault
	}
	switch tf {
	case tfCaret:
		firstDelim = tfCaretFirstDelim
		kvDelim = tfCaretKVDelim
		tagsDelim = tfCaretTagsDelim

	case tfGraphite:
		firstDelim = tfGraphiteFirstDelim
		kvDelim = tfGraphiteKVDelim
		tagsDelim = tfGraphiteTagsDelim

	case tfURI:
		firstDelim = tfURIFirstDelim
		kvDelim = tfURIKVDelim
		tagsDelim = tfURITagsDelim

	case tfPretty:
		firstDelim = tfPrettyFirstDelim
		kvDelim = tfPrettyKVDelim
		tagsDelim = tfPrettyTagsDelim

	}
	return firstDelim, kvDelim, tagsDelim
}

type tagStruct struct {
	Key string
	Val string
}

func (t tagStruct) StringType(tf uint) string {
	_, kv, _ := tagsDelims(tf)
	return fmt.Sprintf("%s%s%s", t.Key, kv, t.Val)
}
func (t tagStruct) String() string {
	return t.StringType(tfDefault)
}

// tagSlice implements sort.Interface for []tagStruct based on
// the Key field.
type tagSlice []tagStruct

func (t tagSlice) Len() int           { return len(t) }
func (t tagSlice) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t tagSlice) Less(i, j int) bool { return t[i].Key < t[j].Key }
func (t tagSlice) StringType(tf uint) string {
	_, _, betweenTags := tagsDelims(tf)
	slice := []string{}
	for _, k := range t {
		slice = append(slice, k.StringType(tf))
	}
	return strings.Join(slice, betweenTags)
}

func (t tagSlice) String() string {
	return t.StringType(tfDefault)
}

func tagsToSortedSlice(tagMap map[string]string) tagSlice {
	tags := make(tagSlice, 0, len(tagMap))
	for t := range tagMap {
		tags = append(tags, tagStruct{Key: t, Val: tagMap[t]})
	}
	sort.Sort(tags)
	return tags
}

func addTags(t1, t2 map[string]string) map[string]string {
	// add t2 + t1
	// t1 overwrites tags in t2, as t1 is more important
	// t1 - bucket tags, t2 extra tags
	for k, v := range t1 {
		t2[k] = v
	}
	return t2
}
func normalizeTags(t map[string]string, tf uint) string {

	return tagsToSortedSlice(t).StringType(tf)
}

func parseExtraTags(tagsStr string) (map[string]string, error) {
	// Only last error is returned
	// It is the caller responsiblity to use this error or ignore
	var err error

	err = nil
	tags := make(map[string]string)

	tagsSlice := strings.Split(tagsStr, " ")
	if len(tagsSlice) == 1 && tagsSlice[0] == "" {
		// it's OK to have empty tags
		return tags, nil
	}
	for _, e := range tagsSlice {
		tagAndVal := strings.Split(e, "=")
		if len(tagAndVal) != 2 || tagAndVal[0] == "" || tagAndVal[1] == "" {
			err = fmt.Errorf("Error: invalid tag format %v (%v)", tagsSlice[1:], tagsSlice)
			log.Printf("%s", err)
		} else {
			tags[strings.TrimSpace(tagAndVal[0])] = strings.TrimSpace(tagAndVal[1])
		}
	}
	return tags, err
}
func parseBucketAndTags(name string) (string, map[string]string, error) {
	// split name in format
	// measure.name.^tag1=val1.^tag2=val2
	// this function can be extended for new "combined" formats
	// FIXME : add other formats

	tags := make(map[string]string)
	// Caret delim is the same for fiest delim and the others
	tfCaretDelim := tfCaretFirstDelim

	tagsSlice := strings.Split(name, tfCaretDelim)
	if len(tagsSlice) == 0 || tagsSlice[0] == "" {
		return "", nil, fmt.Errorf("Format error: Invalid bucket name in \"%s\"", name)
	}
	for _, e := range tagsSlice[1:] {
		tagAndVal := strings.Split(e, "=")
		if len(tagAndVal) != 2 || tagAndVal[0] == "" || tagAndVal[1] == "" {
			log.Printf("Error: invalid tag format [%s] %v ", name, tagsSlice[1:])
		} else {
			tags[tagAndVal[0]] = tagAndVal[1]
		}
	}
	return tagsSlice[0], tags, nil
}

// func main() {
// 	a := map[string]string{"k20": "20", "a10": "10", "11": "11"}
// 	fmt.Printf("MAIN: type %d %s\n", tfDefault, normalizeTags(a,tfDefault))
// 	tfDefault = tfGraphite
// 	fmt.Printf("MAIN: type %d %s\n", tfDefault, normalizeTags(a,tfDefault))
// 	tfDefault = tfURI
// 	fmt.Printf("MAIN: type %d %s\n", tfDefault, normalizeTags(a,tfDefault))
//
// }
