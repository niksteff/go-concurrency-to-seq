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

	sum := 0

	opt := operation.Options{
		Max:         1_000_000,
		Concurrency: 5,
		Operation: func(ctx context.Context, incr int) error {
			sum += incr
			return nil
		},
	}

	err := operation.Do(ctx, opt)

	if err!= nil {
		log.Printf("erro: %s", err.Error())
		return
	}

	log.Println("all good.")
}


