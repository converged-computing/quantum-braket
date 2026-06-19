REGISTRY := ghcr.io/converged-computing
TAG      := latest
 
# gateway.py is maintained in docker/braket-gateway/ and copied into
# docker/optimizer/ at build time. Always sync before building.
.PHONY: build push all load
  
build:
	docker build -t $(REGISTRY)/quantum-braket-problem-generator:$(TAG) docker/problem-generator/
	docker build -t $(REGISTRY)/quantum-braket-transpiler:$(TAG)        docker/transpiler/
	docker build -t $(REGISTRY)/quantum-braket-gateway:$(TAG)    docker/gateway/
	docker build -t $(REGISTRY)/quantum-braket-optimizer:$(TAG)         docker/optimizer/
	docker build -t $(REGISTRY)/quantum-braket-ahs-problem-generator:$(TAG) docker/ahs-problem-generator/
	docker build -t $(REGISTRY)/quantum-braket-ahs-gateway:$(TAG)       docker/ahs-gateway/
	docker build -t $(REGISTRY)/quantum-braket-mis-postprocessor:$(TAG) docker/mis-postprocessor/
 
push: build
	docker push $(REGISTRY)/quantum-braket-problem-generator:$(TAG)
	docker push $(REGISTRY)/quantum-braket-transpiler:$(TAG)
	docker push $(REGISTRY)/quantum-braket-gateway:$(TAG)
	docker push $(REGISTRY)/quantum-braket-optimizer:$(TAG)
	docker push $(REGISTRY)/quantum-braket-ahs-problem-generator:$(TAG)
	docker push $(REGISTRY)/quantum-braket-ahs-gateway:$(TAG)
	docker push $(REGISTRY)/quantum-braket-mis-postprocessor:$(TAG)

load:
	kind load docker-image $(REGISTRY)/quantum-braket-problem-generator:$(TAG)
	kind load docker-image $(REGISTRY)/quantum-braket-transpiler:$(TAG)
	kind load docker-image $(REGISTRY)/quantum-braket-gateway:$(TAG)
	kind load docker-image $(REGISTRY)/quantum-braket-optimizer:$(TAG)
	kind load docker-image $(REGISTRY)/quantum-braket-ahs-problem-generator:$(TAG)
	kind load docker-image $(REGISTRY)/quantum-braket-ahs-gateway:$(TAG)
	kind load docker-image $(REGISTRY)/quantum-braket-mis-postprocessor:$(TAG)

all: push load
