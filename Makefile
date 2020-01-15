#!/bin/bash

## Gitlab
#REGISTRY=registry.gitlab.com/aptrust
#REPOSITORY=container-registry
REGISTRY=docker.io
NAME=exchange
REVISION:=$(shell git rev-parse --short=7 HEAD)
BRANCH = $(subst /,_,$(shell git rev-parse --abbrev-ref HEAD))
PUSHBRANCH = $(subst /,_,$(TRAVIS_BRANCH))
APP_LIST:=$(wildcard apps/apt_*)
APPS_LIST:=$(APP_LIST:apps/%=%)
TAG=$(name):$(REVISION)

DOCKER_TAG_NAME=${REVISION}-${BRANCH}

ifdef TRAVIS
override BRANCH=$(PUSHBRANCH)
endif

#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish release push clean run

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

docker_login:
	if [ ! -z "$(DOCKER_USER)" ]; then echo $(DOCKER_PWD) | docker login --username $(DOCKER_USER) --password-stdin || docker login $(REGISTRY); fi


lsdirs: ## Show the apps that will be built
	@for app in $(APP_LIST:apps/%=%); do \
		echo $$app; \
	done

revision: ## Show me the git hash
	@echo "Revision: ${REVISION}"
	@echo "Branch: ${BRANCH}"

build: ## Build the Exchange containers
	@echo "Branch: ${BRANCH}"
	@for app in $(APP_LIST:apps/%=%); do \
		echo $$app; \
		# Gitlab only
	    # docker build --build-arg EX_SERVICE=$$app -t aptrust/$(NAME)_$$app -t $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app:$(REVISION)-$(BRANCH) -f Dockerfile-build .;
		docker build --build-arg EX_SERVICE=$$app -t aptrust/$(NAME)_$$app -t aptrust/$(NAME)_$$app:$(REVISION)-$(BRANCH) -f Dockerfile-build .; \
	done

up: ## Start Exchange+NSQ containers
	docker-compose up

stop: ## Stop Exchange+NSQ containers
	docker-compose stop

down: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	docker-compose down -v

run: ## Run Exchange service in foreground
	docker run aptrust/$(NAME)_$(filter-out $@, $(MAKECMDGOALS))

runcmd: ## Run a one time command. Takes exchange service name as argument.
	@echo "Need to pass in exchange service and cmd. e.g. make runcmd apt_record bash"
	docker run -it aptrust/$(NAME)_$(filter-out $@, $(MAKECMDGOALS))

%:
	@:

unittest: ## Run unit tests in non Docker setup
	go test ./...

test-ci: ## Run unit tests in CI
	docker run exchange-ci-test

publish: docker_login
#	@echo $(DOCKER_PWD) | docker login -u $(DOCKER_USER) --password-stdin $(REGISTRY)
#	docker login $(REGISTRY)
	@for app in $(APP_LIST:apps/%=%); \
	do \
		echo "Publishing $$app:$(REVISION)-$(BRANCH)"; \
		docker push aptrust/$(NAME)_$$app:$(REVISION)-$(BRANCH);\
	done

#publish-ci:
#	@echo $(DOCKER_PWD) | docker login -u $(DOCKER_USER) --password-stdin $(REGISTRY)
#	@for app in $(APP_LIST:apps/%=%); \
	do \
	echo "Publishing $$app:$(REVISION)-$(PUSHBRANCH)"; \
		docker push $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app:$(REVISION)-$(PUSHBRANCH);\
	done

# Docker release - build, tag and push the container
release: build publish ## Create a release by building and publishing tagged containers to Gitlab

# Docker release - build, tag and push the container
release-ci: build publish-ci ## Create a release by building and publishing tagged containers to Gitlab


push: ## Push the Docker image up to the registry
#	docker push  $(registry)/$(repository)/$(tag)
	@echo "TBD"

clean: ## Clean the generated/compiles files
	@echo "TBD"
