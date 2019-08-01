#!/bin/bash

REGISTRY="registry.gitlab.com/aptrust"
REPOSITORY="container-registry"
NAME="exchange"
VERSION="latest"
TAG="$(name):$(version)"
REVISION:="$(shell git rev-parse --short=2 HEAD)"
APP_LIST:=$(wildcard apps/apt_*)
APPS_LIST:=$(APP_LIST:apps/%=%)

#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

lsdirs: ## Show the apps that will be built
	@for app in $(APP_LIST:apps/%=%); do \
		echo $$app; \
	done
	echo $(APPS_LIST)

revision: ## Show me the git hash
	echo "${REVISION}"

build: ## Build the Exchange containers
	@for app in $(APP_LIST:apps/%=%); do \
		echo $$app; \
		docker build --build-arg EX_SERVICE=$$app -t aptrust/$(NAME)_$$app -t $(NAME)_$$app:$(REVISION) -t $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app -f Dockerfile-build .; \
	done

up: ## Start Exchange+NSQ containers
	sudo docker-compose up -d

stop: ## Stop Exchange+NSQ containers
	sudo docker-compose stop

destroy: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	sudo docker-compose down

run: ## Run Exchange services in foreground
	sudo docker-compose up


publish:
#	docker tag aptrust/ registry.gitlab.com/aptrust/container-registry/pharos && \
#	docker push registry.gitlab.com/aptrust/container-registry/pharos
	docker login $(REGISTRY)
	@for app in $(APPS_LIST); do \
		@echo "Pushing $$app;" \
		docker push $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app\
	done

# Docker release - build, tag and push the container
release: build publish ## Make a release by building and publishing the `{version}` as `latest` tagged containers to Gitlab

push: ## Push the Docker image up to the registry
#	docker push  $(registry)/$(repository)/$(tag)
	"TBD"

clean: ## Clean the generated/compiles files
