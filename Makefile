# -*- Makefile -*-


# Possible targets: coordinator, worker.
%:
	go build -o bin/$@ cmd/$@/main.go
