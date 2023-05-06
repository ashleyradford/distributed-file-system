package main

import (
	"dfs/util"
	"os"
	"strconv"
	"strings"
)

type mapReduce string

func (m mapReduce) Map(line_number int, line_text string, context *util.Context) error {
	words := strings.Fields(line_text)
	if len(words) > 0 {
		for _, word := range words {
			word = strings.ToLower(strings.Trim(word, ".,!?"))
			err := context.Write(word, "1")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m mapReduce) Reduce(key string, values []string, context *os.File) error {
	sum := 0
	for _, value := range values {
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		sum += v
	}

	// write key value
	context.Write(append([]byte(key), []byte("\t")...))
	// write aggregated value
	context.Write(append([]byte(strconv.Itoa(sum)), []byte("\n")...))

	return nil
}

// var MapReduce *jobs.Behavior
var MapReduce mapReduce
