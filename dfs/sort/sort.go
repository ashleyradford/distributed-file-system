package main

import (
	"dfs/util"
	"os"
	"strconv"
	"strings"
)

type mapReduce string

func (m mapReduce) Map(line_number int, line_text string, context *util.Context) error {
	tokens := strings.Fields(line_text)
	if len(tokens) > 0 {
		word := tokens[0]
		count := tokens[1]
		value, err := strconv.Atoi(count)
		if err != nil {
			return err
		}
		if value > 40 {
			err := context.Write(count, word)
			if err != nil {
				return err
			}
		}
	}
	context.SetIntCompare()
	return nil
}

func (m mapReduce) Reduce(key string, values []string, context *os.File) error {
	for i := range values {
		// write key value
		context.Write(append([]byte(key), []byte("\t")...))
		// write each word
		context.Write(append([]byte(values[i]), []byte("\n")...))
	}
	return nil
}

var MapReduce mapReduce
