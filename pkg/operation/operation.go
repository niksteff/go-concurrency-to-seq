package operation

import (
	"context"
	"log"
	"sync"
)

type Options struct {
	Max         int
	Concurrency int
	Operation   func(context.Context, int, *int) error
}

func Operate(ctx context.Context, opt Options) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	in := make(chan func() error, opt.Concurrency)
	out := make(chan error)

	var firstErr error

	// error handler
	var wgErr sync.WaitGroup
	wgErr.Add(1)
	go func(wg *sync.WaitGroup, out <-chan error) {
		defer wg.Done()
		defer cancel()

		for e := range out {
			if e != nil {
				log.Printf("receiving error: %s", e.Error())
			}
			if e != nil && firstErr == nil {
				log.Printf("storing first error: %s", e.Error())
				firstErr = e
			}
		}
	}(&wgErr, out)

	// worker
	var wg sync.WaitGroup
	for i := 0; i < opt.Concurrency; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, id int, ctx context.Context, in <-chan func() error, out chan<- error) {
			defer wg.Done()
			defer log.Printf("worker #%d: stopping", id)
			log.Printf("worker #%d: starting", id)

			for {
				select {
				case <-ctx.Done():
					return
				case op, ok := <-in:
					if !ok {
						return
					}

					out <- op()
				}
			}
		}(&wg, i, ctx, in, out)
	}

	// generator
	sum := 0
	for i := 0; i < opt.Max; i++ {
		if firstErr != nil {
			log.Printf("generator: stopping")
			break
		}

		in <- func() error {
			return opt.Operation(ctx, 1, &sum)
		}
	}
	close(in)
	wg.Wait()
	
	close(out)
	wgErr.Wait()

	return firstErr
}