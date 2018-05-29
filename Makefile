
GOTEST ?= go test
TARGET := .

test:
	@$(GOTEST) -run=$(TARGET) -v .
