#!/bin/bash

REGISTRY=registry.gitlab.com/aptrust
REPOSITORY=container-registry
NAME=exchange
REVISION:=$(shell git rev-parse --short=7 HEAD)
BRANCH = $(subst /,_,$(shell git rev-parse --abbrev-ref HEAD))
PUSHBRANCH = $(subst /,_,$(TRAVIS_BRANCH))
APP_LIST:=$(wildcard apps/apt_*)
APPS_LIST:=$(APP_LIST:apps/%=%)
TAG=$(name):$(REVISION)

#
# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build publish release push clean

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
		docker build --build-arg EX_SERVICE=$$app -t aptrust/$(NAME)_$$app -t $(NAME)_$$app:$(REVISION) -t $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app:$(REVISION)-$(BRANCH) -f Dockerfile-build .; \
	done

up: ## Start Exchange+NSQ containers
	DOCKER_TAG_NAME=$(REVISION) docker-compose up

stop: ## Stop Exchange+NSQ containers
	docker-compose stop

down: ## Stop and remove all Exchange+NSQ containers, networks, images, and volumes
	docker-compose down -v

run: ## Run Exchange services in foreground
	docker-compose up

publish:
#	docker tag aptrust/ registry.gitlab.com/aptrust/container-registry/pharos && \
#	docker push registry.gitlab.com/aptrust/container-registry/pharos
	docker login $(REGISTRY)
	@for app in $(APPS_LIST); do \
		@echo "Pushing $$app;" \
		docker push $(REGISTRY)/$(REPOSITORY)/$(NAME)_$$app\
	done

publish-ci:
	@echo $(DOCKER_PWD) | docker login -u $(DOCKER_USER) --password-stdin $(REGISTRY)
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
