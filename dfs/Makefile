all: ${HOME}/go/bin/controller ${HOME}/go/bin/yarm ${HOME}/go/bin/storage_node ${HOME}/go/bin/client

${HOME}/go/bin/controller: controller/controller.go
	go install controller/controller.go

${HOME}/go/bin/yarm: yarm/yarm.go
	go install yarm/yarm.go

${HOME}/go/bin/storage_node: storage_node/storage_node.go
	go install storage_node/storage_node.go

${HOME}/go/bin/client: client/client.go
	go install client/client.go

clean:
	rm -rf ${HOME}/go/bin/{controller, storage_node, client}
