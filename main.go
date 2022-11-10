package main

import (
	"context"
	"log"
	"time"

	"example.com/go-concurrency/pkg/operation"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opt := operation.Options{
		Max:         1_000_000,
		Concurrency: 5,
		Operation: func(ctx context.Context, incr int, sum *int) error {
			log.Printf("sum: %d, incr: %d", *sum, incr)
			*sum += incr
			log.Printf("result: %d", sum)
			return nil
		},
	}

	operation.Operate(ctx, opt)
}


