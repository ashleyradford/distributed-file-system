#!/usr/bin/env bash

# Requires google.golang.org/protobuf/cmd/protoc-gen-go@latest

PATH="$PATH:${GOPATH}/bin:${HOME}/go/bin" protoc --go_out=./ proto/*.proto
