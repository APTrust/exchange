#!/bin/bash

REGISTRY="registry.gitlab.com/aptrust"
REPOSITORY="container-registry"
NAME="exchange"
VERSION="latest"
TAG="$(name):$(version)"
REVISION:="$(shell git rev-parse --short=2 HEAD)"
APP_LIST:=$(wildcard apps/apt_*)

#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

lsdirs: ## Show the apps that will be built
	@for folder in $(APP_LIST:apps/%=%); do \
		echo $$folder; \
	done

revision: ## Show me the git hash
	echo "${REVISION}"

build: ## Build the Exchange containers
	@for folder in $(APP_LIST:apps/%=%); do \
		echo $$folder; \
		docker build --build-arg EX_SERVICE=$$folder -t aptrust/$(NAME)_$$folder -t $(NAME)_$$folder:$(REVISION) -t $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$folder -f Dockerfile-build .; \
	done

up: ## Start Exchange+NSQ containers
	sudo docker-compose -p exchange up -d

stop: ## Stop Exchange+NSQ containers
	sudo docker-compose -p exchange stop

destroy: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	sudo docker-compose -p exchange down

run: ## Run Exchange services in foreground
	sudo docker-compose -p exchange up


publish:
#	docker tag aptrust/ registry.gitlab.com/aptrust/container-registry/pharos && \
#	docker push registry.gitlab.com/aptrust/container-registry/pharos
	"Need loop support for each app"

# Docker release - build, tag and push the container
release: build publish ## Make a release by building and publishing the `{version}` as `latest` tagged containers to Gitlab

push: ## Push the Docker image up to the registry
#	docker push  $(registry)/$(repository)/$(tag)
	"TBD"

clean: ## Clean the generated/compiles files
