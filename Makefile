.PHONY: all
all: build local-docker

.PHONY: format
format:
	mvn spring-javaformat:apply

.PHONY: build
build: format
	mvn clean compile

.PHONY: local-docker
local-docker: build
	mvn jib:dockerBuild

.PHONY: publish-docker
publish-docker: build
	mvn jib:build
