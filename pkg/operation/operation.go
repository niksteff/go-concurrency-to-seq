package operation

import (
	"context"
	"log"
	"sync"
)

type Options struct {
	Max         int
	Concurrency int
	Incr        int
	Operation   func(context.Context, int) error
}

func Operate(ctx context.Context, opt Options) <-chan error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	in := make(chan func() error, opt.Concurrency)
	out := make(chan error)

	// var firstErr error

	// error handler
	// var wgErr sync.WaitGroup
	// wgErr.Add(1)
	// go func(ctx context.Context, wg *sync.WaitGroup, out <-chan error) {
	// 	defer wg.Done()
	// 	defer cancel()

	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		case e, ok := <-out:
	// 			if !ok {
	// 				return
	// 			}
	// 			if e != nil && firstErr != nil {
	// 				log.Printf("another error: %s", e.Error())
	// 			}
	// 			if e != nil && firstErr == nil {
	// 				log.Printf("storing first error: %s", e.Error())
	// 				firstErr = e
	// 				cancel()
	// 			}
	// 		}
	// 	}
	// }(ctx, &wgErr, out)

	// worker
	var wg sync.WaitGroup
	for i := 0; i < opt.Concurrency; i++ {
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, id int, in <-chan func() error, out chan<- error) {
			defer wg.Done()
			defer log.Printf("worker #%d: stopping", id)
			log.Printf("worker #%d: starting", id)

			for {
				select {
				case <-ctx.Done():
					log.Printf("worker #%d: ctx.Done", id)
					return
				case op, ok := <-in:
					if !ok {
						return
					}
					// log.Printf("worker #%d: operating", id)
					out <- op()
				}
			}
		}(ctx, &wg, i, in, out)
	}

	// generator
	var wgGen sync.WaitGroup
	wgGen.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup, in chan<- func() error) {
		for i := 0; i < opt.Max; i++ {
			select {
			case <-ctx.Done():
				log.Printf("generator: ctx.Done")
				return
			default:
				// log.Printf("generator: generating")
				in <- func() error {
					return opt.Operation(ctx, opt.Incr)
				}
			}
		}

		close(in)
		wg.Wait()
		close(out)
	}(ctx, &wg, in)

	return out
}
