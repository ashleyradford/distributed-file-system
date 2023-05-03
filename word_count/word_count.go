package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type mapReduce string

func (m mapReduce) Map(line_number int, line_text string) map[string]string {
	kvPairs := make(map[string]string)
	words := strings.Fields(line_text)
	if len(words) > 0 {
		for _, word := range words {
			_, ok := kvPairs[word]
			if !ok {
				kvPairs[word] = "1"
			} else {
				count, err := strconv.Atoi(kvPairs[word])
				if err != nil {
					log.Println(err)
					return nil
				}
				kvPairs[word] = strconv.Itoa(count + 1)
			}
		}
	}

	return kvPairs
}

func (m mapReduce) Reduce() {
	fmt.Printf("Reduce >.<")
}

var MapReduce mapReduce
