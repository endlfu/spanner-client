
TESTPKGS = $(shell go list ./ ./admin)

GOTEST ?= go test
TARGET := .

test:
	@$(GOTEST) -v -race -run=$(TARGET) $(TESTPKGS)