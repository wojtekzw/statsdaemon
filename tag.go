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
	default:
		log.Fatalf("Unknown tag format %d. Setting to default = %d", tf, tfDefault)

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

	// make copy of t2 in t3

	t3 := make(map[string]string)

	for k, v := range t2 {
		t3[k] = v
	}

	// add/overwrite t3 by t1
	for k, v := range t1 {
		t3[k] = v
	}
	return t3
}

func normalizeTags(t map[string]string, tf uint) string {
	return tagsToSortedSlice(t).StringType(tf)
}

func parseExtraTags(tagsStr string) (map[string]string, error) {

	tags := make(map[string]string)

	tagsSlice := strings.Split(tagsStr, " ")
	if len(tagsSlice) == 1 && tagsSlice[0] == "" {
		// it's OK to have empty tags
		return tags, nil
	}
	for _, e := range tagsSlice {
		tagAndVal := strings.Split(e, "=")
		if len(tagAndVal) != 2 || tagAndVal[0] == "" || tagAndVal[1] == "" {
			return make(map[string]string), fmt.Errorf("Invalid tag format %v (%v)", tagsSlice[1:], tagsSlice)
		}
		tags[strings.TrimSpace(tagAndVal[0])] = strings.TrimSpace(tagAndVal[1])

	}
	return tags, nil
}
func parseBucketAndTags(name string) (string, map[string]string, error) {
	// split name in format
	// measure.name.^tag1=val1.^tag2=val2
	// this function can be extended for new "combined" formats
	// FIXME : add other formats

	logCtx := log.WithFields(log.Fields{
		"in": "parseBucketAndTags",
	})

	tags := make(map[string]string)
	// Caret delim is the same for first delim and the others
	tfCaretDelim := tfCaretFirstDelim

	tagsSlice := strings.Split(name, tfCaretDelim)
	if len(tagsSlice) == 0 || tagsSlice[0] == "" {
		return "", nil, fmt.Errorf("Format error: Invalid bucket name in \"%s\"", name)
	}

	if strings.HasSuffix(name, ".") {
		return "", nil, fmt.Errorf("Format error: Invalid bucket name in \"%s\"", name)
	}

	if strings.Index(name, "..") > -1 {
		return "", nil, fmt.Errorf("Format error: Invalid bucket name in \"%s\"", name)
	}

	//FIXME - what is this ? (why  "=^")
	if strings.IndexAny(tagsSlice[0], "=^") > -1 {
		oldBucket := tagsSlice[0]
		tagsSlice[0] = sanitizeBucket(tagsSlice[0])
		logCtx.Errorf("Format error: Converting bucket name from  \"%s\" (in %s) to \"%s\"", oldBucket, name, tagsSlice[0])
		Stat.PointsParseSoftFailInc()
	}

	for _, e := range tagsSlice[1:] {
		tagAndVal := strings.Split(e, "=")
		if len(tagAndVal) != 2 || tagAndVal[0] == "" || tagAndVal[1] == "" {
			logCtx.Errorf("Format error: Invalid tag format [%s] %v, Removing tag", name, tagsSlice[1:])
			Stat.PointsParseSoftFailInc()
		} else {
			tags[tagAndVal[0]] = tagAndVal[1]
		}
	}
	return tagsSlice[0], tags, nil
}
