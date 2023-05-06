package util

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

type Context struct {
	files      []*os.File
	filenames  []string
	stringSort bool
}

func (c *Context) determineNode(key string) int {
	// get key and node location
	fnvHash := fnv.New32a()
	fnvHash.Write([]byte(key))
	idx := fnvHash.Sum32() % uint32(len(c.filenames))
	return int(idx)
}

func NewContext(dest string, nodeAddrs []string, stringSort bool) (*Context, error) {
	// create context
	c := &Context{
		files:      make([]*os.File, 0),
		filenames:  make([]string, 0),
		stringSort: stringSort,
	}

	// for each dest, open a file
	for _, nodeAddr := range nodeAddrs {
		file, err := ioutil.TempFile(dest, fmt.Sprintf("%s_pairs_*", nodeAddr))
		if err != nil {
			return nil, err
		}

		c.files = append(c.files, file)
		c.filenames = append(c.filenames, file.Name())
	}

	return c, nil
}

func (c *Context) Write(key string, value string) error {
	nodeIdx := c.determineNode(key)
	if _, err := c.files[nodeIdx].Write(append([]byte(key), []byte("\t")...)); err != nil {
		return err
	}
	if _, err := c.files[nodeIdx].Write(append([]byte(value), []byte("\n")...)); err != nil {
		return err
	}
	return nil
}

func (c *Context) SetIntCompare() {
	c.stringSort = false
}

func (c *Context) GetFilenames() []string {
	return c.filenames
}

func (c *Context) IsStringCompare() bool {
	return c.stringSort
}

func (c *Context) CloseFiles() {
	for _, f := range c.files {
		f.Close()
	}
}

func (c *Context) RemoveFiles() {
	for _, name := range c.filenames {
		os.Remove(name)
	}
}
