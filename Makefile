.PHONY: all
all: build docker_local

.PHONY: format
format:
	mvn spring-javaformat:apply

.PHONY: build
build: format
	mvn clean compile

.PHONY: test
test: build
	mvn test

.PHONY: docker_local
docker_local: build
	mvn jib:dockerBuild

.PHONY: docker_hub_image
docker_hub_image: build
	mvn jib:build

.PHONY: dc_up_es
dc_up_es:
	docker-compose -f docker-compose-elasticsearch.yml up -d

.PHONY: dc_up_cassandra
dc_up_cassandra:
	docker-compose -f docker-compose-cassandra.yml up -d

.PHONY: release
release:
	mvn release:prepare
	mvn release:perform