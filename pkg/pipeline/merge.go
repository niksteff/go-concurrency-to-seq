package pipeline

import (
	"context"
	"log"
	"sync"
)

// Merge the given channels into the output channel
func Merge[T any](ctx context.Context, in ...<-chan T) <-chan T {
	out := make(chan T)

	// for each incoming channel create a go routine to merge the incoming
	// values onto our out channel
	var wg sync.WaitGroup
	for _, c := range in {
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, c <-chan T) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-c:
					if !ok {
						// if the channel is closed return
						return
					}
					
					// write the value to the output channel
					out <- v
				}
			}
		}(ctx, &wg, c)
	}

	// wait for the merge to complete before closing the out channel
	go func(wg *sync.WaitGroup) {
		log.Println("merge: waiting")
		wg.Wait()
		close(out)
		log.Println("merge: closed")
	}(&wg)

	return out
}
