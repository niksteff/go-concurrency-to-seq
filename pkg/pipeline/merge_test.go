package pipeline_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	"example.com/go-concurrency/pkg/pipeline"
)

func TestMerge(t *testing.T) {
	numWorkers := 10
	numOfCounts := 10
	expectedValues := numOfCounts * numWorkers

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// for our input channels
	inputChannels := make([]<-chan int, 0, numWorkers)
	errChannels := make([]<-chan error, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		data, errs := numberCounter(ctx, 0, numOfCounts)

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
	numOfCounts := 1001

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// for our input channels
	inputChannels := make([]<-chan int, 0, numWorkers)
	errChannels := make([]<-chan error, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		data, errs := numberCounter(ctx, 1001, numOfCounts)

		inputChannels = append(inputChannels, data)
		errChannels = append(errChannels, errs)
	}

	// now merge all our input data
	mergedData := pipeline.Merge(ctx, inputChannels...)

	// now merge all our input channels into one mergedErrs channel
	mergedErrs := pipeline.Merge(ctx, errChannels...)

	_, errs := sink(ctx, cancel, mergedData, mergedErrs)
	if len(errs) != 10 {
		t.Errorf("expected to receive at least 10 error but got %d", len(errs))
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

// numberCounter will generate n integers between min, max
func numberCounter(ctx context.Context, start int, n int) (<-chan int, <-chan error) {
	out := make(chan int)
	errs := make(chan error)

	maxAllowed := 1000

	go func() {
		defer close(out)
		defer close(errs)

		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				if start > maxAllowed {
					err := MaxError{
						Allowed: maxAllowed,
						Given:   start,
					}
					errs <- err
					return
				}

				// just count up as many times as the user wants
				out <- i
			}
		}
	}()

	return out, errs
}

func sink(ctx context.Context, cancel context.CancelFunc, data <-chan int, errs <-chan error) (int, []error) {
	counter := 0
	aggrErrs := make([]error, 0)

	okData, okErr := true, true
	// var d int
	var err error

loop:
	for {
		select {
		case <-ctx.Done():
			log.Printf("sink: ctx done: %v, data: %d", ctx.Err().Error(), len(data))
			return counter, aggrErrs
		case _, okData = <-data:
			if okData {
				// log.Printf("data: %d", d)
				counter++
			} else if !okErr {
				break loop
			}
		case err, okErr = <-errs:
			if okErr {
				log.Printf("reading err")
				if err != nil {
					aggrErrs = append(aggrErrs, err)
				}
			} else if !okData {
				break loop
			}
		}
	}

	if !okData && !okErr {
		log.Printf("cancelling context => okData: %t, okErr: %t", okData, okErr)
		cancel()
	}

	return counter, aggrErrs
}
