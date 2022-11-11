package pipeline_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"example.com/go-concurrency/pkg/pipeline"
)

func TestMerge(t *testing.T) {
	numWorkers := 10
	numOfValues := 10
	expectedValues := numOfValues * numWorkers

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// for our input channels
	inputChannels := make([]<-chan int, 0, numWorkers)
	errChannels := make([]<-chan error, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		data, errs := numberGenerator(ctx, 0, 10, numOfValues)

		inputChannels = append(inputChannels, data)
		errChannels = append(errChannels, errs)
	}

	// now merge all our input channels into one merged channel
	mergedData := pipeline.Merge(ctx, inputChannels...)
	mergedErrs := pipeline.Merge(ctx, errChannels...)

	// now read all merged results and check if the got matches the expected
	// number of results
	got, _ := sink(ctx, cancel, mergedData, mergedErrs)
	if got != expectedValues {
		t.Errorf("expected %d values but got %d values", expectedValues, got)
	}
}

func TestMergeError(t *testing.T) {
	numWorkers := 10
	numOfValues := 10

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// for our input channels
	inputChannels := make([]<-chan int, 0, numWorkers)
	errChannels := make([]<-chan error, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		data, errs := numberGenerator(ctx, 0, 1000, numOfValues)

		inputChannels = append(inputChannels, data)
		errChannels = append(errChannels, errs)
	}

	// now merge all our input data
	mergedData := pipeline.Merge(ctx, inputChannels...)

	// now merge all our input channels into one mergedErrs channel
	mergedErrs := pipeline.Merge(ctx, errChannels...)

	_, errs := sink(ctx, cancel, mergedData, mergedErrs)
	if len(errs) < 1 {
		t.Errorf("expected to receive at least 1 error but got %d", len(errs))
	}

	t.Logf("len errors: %d", len(errs))
	for idx, e := range errs {
		t.Logf("idx #%d: %s", idx, e.Error())
	}
}

type MaxError struct {
	error
	Allowed int
	Given   int
}

func (e MaxError) Error() string {
	return fmt.Sprintf("the max value %d is greater than the allowed value of %d", e.Given, e.Allowed)
}

// numberGenerator will generate n integers between min, max
func numberGenerator(ctx context.Context, min int, max int, n int) (<-chan int, <-chan error) {
	out := make(chan int)
	errs := make(chan error)

	maxAllowed := 999

	go func() {
		defer close(out)
		defer close(errs)

		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				if max > maxAllowed {
					err := MaxError{
						Allowed: maxAllowed,
						Given:   max,
					}
					errs <- err
					return
				}

				rand.Seed(time.Now().UnixNano())
				val := rand.Intn(max-min+1) + min
				out <- val 
			}
		}
	}()

	return out, errs
}

func sink(ctx context.Context, cancel context.CancelFunc, data <-chan int, errs <-chan error) (int, []error) {
	counter := 0
	aggrErrs := make([]error, 0)

	for {
		select {
		case _, ok := <-data:
			if ok {
				counter++
			}
		case err, ok := <-errs:
			if !ok {
				cancel()
			}
			if err != nil {
				aggrErrs = append(aggrErrs, err)
			}
		case <-ctx.Done():
			log.Printf("sink: ctx done: %v", ctx.Err().Error())
			return counter, aggrErrs
		}
	}
}
