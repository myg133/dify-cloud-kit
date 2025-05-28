package oss

import "fmt"

var (
	ErrProviderNotFound = NewCloudKitError("provider not found", "")
	ErrArgumentInvalid  = NewCloudKitError("argument invalid", "")
	ErrProviderInit     = NewCloudKitError("provider init error", "")
)

type CloudKitError struct {
	Reason string
	Detail string
	Err    error
}

func NewCloudKitError(reason string, detail string) *CloudKitError {
	return &CloudKitError{
		Reason: reason,
		Detail: detail,
	}
}

func (c *CloudKitError) Error() string {
	if c.Detail != "" {
		return fmt.Sprintf("reason: %s; detail: %s; error: %v", c.Reason, c.Detail, c.Err)
	}
	return fmt.Sprintf("reason: %s; error: %v", c.Reason, c.Err)
}

func (c *CloudKitError) WithDetail(detail string) *CloudKitError {
	c.Detail = detail
	return c
}

func (c *CloudKitError) WithError(err error) *CloudKitError {
	c.Err = err
	return c
}
