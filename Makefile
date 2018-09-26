all: push

GOOS = linux

TAG = latest
REPO = ssalaues/benchio


build:
	GOOS=$(GOOS) go build .

container: build
	docker build -t $(REPO):$(TAG) .

push: container
	docker push $(REPO):$(TAG)

clean:
	docker rmi $(REPO):$(TAG)
	rm ./benchio
