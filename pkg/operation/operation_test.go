package operation_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"example.com/go-concurrency/pkg/operation"
)

type TestSum struct {
	Mtx sync.Mutex
	Sum int
}

func TestOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s := TestSum{
		Sum: 0,
	}

	opt := operation.Options{
		Max:         10_000,
		Incr:        1000,
		Concurrency: 5,
		Operation: func(ctx context.Context, incr int) error {
			defer s.Mtx.Unlock()
			s.Mtx.Lock()

			if s.Sum+incr < 10_000 {
				s.Sum += incr
			}
			return nil
		},
	}

	err := operation.Operate(ctx, opt)
	if err != nil {
		t.Errorf("did not expect error: %s", err.Error())
		t.FailNow()
	}
}

func TestOperationFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stopSize := 5_000
	
	s := TestSum{
		Sum: 0,
	}

	opt := operation.Options{
		Max:         10_000,
		Concurrency: 5,
		Incr:        1000,
		Operation: func(ctx context.Context, incr int) error {
			defer s.Mtx.Unlock()

			select {
			case <-ctx.Done():
				return nil
			default:
				s.Mtx.Lock()
				s.Sum += incr

				if s.Sum > stopSize {
					return fmt.Errorf("size too large at %d", stopSize)
				}

				return nil
			}
		},
	}

	err := operation.Operate(ctx, opt)
	if err == nil {
		t.Error("did expect error but got none!")
		t.FailNow()
	}
}
