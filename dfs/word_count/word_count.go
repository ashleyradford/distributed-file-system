package main

import (
	"dfs/util"
	"fmt"
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

func (m mapReduce) Reduce() {
	fmt.Printf("Reduce >.<")
}

// var MapReduce *jobs.Behavior
var MapReduce mapReduce
