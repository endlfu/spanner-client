
TESTPKGS = $(shell go list ./)

GOTEST ?= go test
TARGET := .

test:
	@$(GOTEST) -v -race -run=$(TARGET) $(TESTPKGS)