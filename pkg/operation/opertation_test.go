package operation_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"example.com/go-concurrency/pkg/operation"
)

func TestOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
 
	opt := operation.Options{
		Max:         10_000,
		Concurrency: 5,
		Operation: func(ctx context.Context, incr int, sum *int) error {
			log.Printf("sum: %d, incr: %d", *sum, incr)
			if *sum + incr < 10_000 {
				*sum += incr
				log.Printf("result: %d", *sum)
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

	opt := operation.Options{
		Max:         10_000,
		Concurrency: 5,
		Operation: func(ctx context.Context, incr int, sum *int) error {
			select {
			case <-ctx.Done():
				return nil
			default:
				// log.Printf("sum: %d, incr: %d", *sum, incr)
				*sum += incr
				// log.Printf("result: %d", *sum)

				if *sum > stopSize {
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
