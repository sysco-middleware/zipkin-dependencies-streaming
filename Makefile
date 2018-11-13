.PHONY: all
all: format build

.PHONY: format
format:
	mvn spring-javaformat:apply

.PHONY: build
build:
	mvn clean compile