package errors

import "github.com/pkg/errors"

func IsClientError(err error) bool {
	e, ok := errors.Cause(err).(clientError)
	return ok && e.ClientError()
}

type clientError interface {
	ClientError() bool
}

type ClientError struct {
	msg string
}

func NewClientError(msg string) *ClientError {
	return &ClientError{
		msg: msg,
	}
}

func (e *ClientError) Error() string {
	return e.msg
}

func (*ClientError) ClientError() bool {
	return true
}
