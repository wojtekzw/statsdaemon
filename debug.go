package main

import "strings"

func prefixPresent(s string, pList []string) bool {
	for _, v := range pList {
		if strings.HasPrefix(s, v) {
			return true
		}
	}
	return false
}
