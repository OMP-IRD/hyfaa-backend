TAG=1.0
IMAGE=hyfaa-backend

all: docker-build docker-push

docker-build:
	docker build -f docker/Dockerfile -t pigeosolutions/${IMAGE}:${TAG} .

docker-push:
	docker push pigeosolutions/${IMAGE}:${TAG}

.PHONY: run
run: env
	$(shell . .venv/bin/activate && flask run)