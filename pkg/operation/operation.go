package operation

import (
	"context"

	"example.com/go-concurrency/pkg/pipeline"
)

type Options struct {
	Max         int
	Concurrency int
	Incr        int
	Operation   func(context.Context, int) error
}

func Do(ctx context.Context, opt Options) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// our errors will be stored in here
	errChans := make([]<-chan error, 0)
	
	// generate the jobs from the options
	jobs := generate(ctx, opt)

	// start the operators concurrently based on the setting
	for i := 0; i < opt.Concurrency; i++ {
		err := operate(ctx, jobs)
		errChans = append(errChans, err)
	}

	// merge the incoming errors from the concurrent calls
	mergedErrs := pipeline.Merge(ctx, errChans...)

	// store our first error and return it
	firstErr := sink(ctx, mergedErrs)

	return firstErr
}

// sink will store the first error to arrive for returning it
func sink(ctx context.Context, in <-chan error) error {
	var firstErr error

	for {
		select {
		case <-ctx.Done():
			return firstErr
		case err, ok := <-in:
			if !ok {
				return firstErr
			}

			if firstErr == nil {
				firstErr = err
			}
		}
	}
}

// operate will perform the incoming actions and write the result to the return chan
func operate(ctx context.Context, in <-chan func() error) <-chan error {
	out := make(chan error)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case op, ok := <-in:
				if !ok {
					return
				}
				err := op()
				if err != nil {
					out <- err
				}
			}
		}
	}()

	return out
}

// generate operations from the options
func generate(ctx context.Context, opt Options) <-chan func() error {
	out := make(chan func() error)

	go func() {
		defer close(out)

		for i := 0; i < opt.Max; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				out <- func() error {
					return opt.Operation(ctx, opt.Incr)
				}
			}
		}
	}()

	return out
}
