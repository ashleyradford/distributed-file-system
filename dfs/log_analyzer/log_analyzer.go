package main

import (
	"dfs/util"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type mapReduce string

/* test files:
 * orion02:/bigdata/mmalensek/logs/url-dataset-1m.txt
 * orion02:/bigdata/mmalensek/logs/url-dataset-10m.txt
 * orion02:/bigdata/mmalensek/logs/url-dataset-100m.txt */

// 2023-01-30  22:10   231.85.130.87   http://autoplanet1.com/redirect/?goto=http://www.formula1.com/results/driver/2007/812.html
// 2023-01-30  22:10   142.215.245.116 https://www.luneriverharp.com/booking.html

func (m mapReduce) Map(line_number int, line_text string, context *util.Context) error {
	pattern, _ := regexp.Compile(`(//www\.|//)([a-zA-Z.]+)/`)
	data := strings.Fields(line_text)
	if len(data) > 0 {
		// grab the domain
		url := data[3]
		matched := pattern.FindStringSubmatch(url)
		domain := ""
		if matched != nil {
			domain = matched[2]
		}

		if domain != "" {
			err := context.Write(domain, "1")
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

var MapReduce mapReduce
