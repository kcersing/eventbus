package eventbus

import (
	"context"
	"errors"
	"testing"
)

func TestUnrecoverable(t *testing.T) {
	base := errors.New("bad payload")
	err := Unrecoverable(base)
	if !IsUnrecoverable(err) {
		t.Fatal("expected ErrUnrecoverable")
	}
	if !errors.Is(err, base) {
		t.Fatal("expected wrapped error to be preserved")
	}
	if IsUnrecoverable(errors.New("normal")) {
		t.Fatal("normal error must not be unrecoverable")
	}
}

func TestDefaultRetryClassifier(t *testing.T) {
	ctx := context.Background()
	event := NewEvent("test", nil)

	if got := DefaultRetryClassifier(ctx, event, errors.New("transient")); got != Retry {
		t.Fatalf("transient decision = %v, want Retry", got)
	}
	if got := DefaultRetryClassifier(ctx, event, Unrecoverable(errors.New("bad"))); got != DeadLetter {
		t.Fatalf("unrecoverable decision = %v, want DeadLetter", got)
	}
}
