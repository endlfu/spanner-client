package errors

import "github.com/pkg/errors"

func IsNotFoundError(err error) bool {
	e, ok := errors.Cause(err).(notFoundError)
	return ok && e.NotFoundError()
}

type notFoundError interface {
	NotFoundError() bool
}

type NotFoundError struct {
	msg string
}

func NewNotFoundError(msg string) *NotFoundError {
	return &NotFoundError{
		msg: msg,
	}
}

func (e *NotFoundError) Error() string {
	return e.msg
}

func (*NotFoundError) NotFoundError() bool {
	return true
}

func (*NotFoundError) ClientError() bool {
	return true
}
