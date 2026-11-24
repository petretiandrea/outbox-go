package domain

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// permanentError marks a failure that retrying cannot fix (invalid message,
// unknown channel, a broker rejecting the payload, ...).
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as a permanent failure. Sources dead-letter messages that
// fail permanently instead of retrying them. Untagged errors are transient.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// MessageFailure is the outcome of one message that could not be published.
type MessageFailure struct {
	ID  string
	Err error
}

// Permanent reports whether the failure must not be retried.
func (f MessageFailure) Permanent() bool { return IsPermanent(f.Err) }

// BatchError is returned by a handler when only part of a batch failed.
// Messages of the batch that are not listed in Failures were published and
// may be acknowledged by the source. Any other non-nil handler error means
// that the whole batch failed transiently.
type BatchError struct {
	Failures []MessageFailure
}

func (e *BatchError) Error() string {
	parts := make([]string, 0, len(e.Failures))
	for _, f := range e.Failures {
		kind := "transient"
		if f.Permanent() {
			kind = "permanent"
		}
		parts = append(parts, fmt.Sprintf("message %q (%s): %v", f.ID, kind, f.Err))
	}
	sort.Strings(parts)
	return fmt.Sprintf("%d message(s) failed: %s", len(e.Failures), strings.Join(parts, "; "))
}
