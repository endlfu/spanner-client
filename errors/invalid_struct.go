package errors

import "github.com/pkg/errors"

func IsInvalidStructError(err error) bool {
	e, ok := errors.Cause(err).(invalidStructError)
	return ok && e.InvalidStructError()
}

type invalidStructError interface {
	InvalidStructError() bool
}

type InvalidStructError struct {
	msg string
}

func NewInvalidStructError(msg string) *InvalidStructError {
	return &InvalidStructError{
		msg: msg,
	}
}

func (e *InvalidStructError) Error() string {
	return e.msg
}

func (*InvalidStructError) InvalidStructError() bool {
	return true
}
