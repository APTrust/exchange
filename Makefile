#!/bin/bash
# Docker_start.sh
# Script to provision a development environment
# with docker-ce and start pharos in a container as per docker-compose.yml

# 1. ID OS - Linux or OSX
# 2. If OSX, install homebrew
# 3. Install Docker-CE on osx (brew cask install docker/linux: apt-get install docker)
# 4. Run make build to build the latest version of Pharos
# 5. docker-compose up -f docker-compose-dev.yml
# 6. Connect to pharos.docker.localhost in your browser.

# -  make restart: docker-compose up -d -f docker-compose-dev.yml
#
registry="registry.gitlab.com/aptrust"
repository="container-registry"
name="exchange"
version="latest"
tag="$(name):$(version)"
REVISION="$(shell git rev-parse --short=2 HEAD)"
#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

revision: ## Show me the git hash
	echo "${REVISION}"

build: ## Build the Exchange containers
#	docker build -t aptrust/$(tag) -t $(tag) -t $(name):$(revision) -t $(registry)/$(repository)/$(tag) .
	echo "Needs loop support for multiple apps"

up: ## Start Exchange+NSQ containers
	docker-compose -p exchange -d up

stop: ## Stop Exchange+NSQ containers
	docker-compose -p exchange -d stop

destroy: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	docker-compose -p exchange down

run: ## Run Exchange services in foreground
	docker-compose -p exchange up


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
