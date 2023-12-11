//go:build !durable

package dispatch

import (
	"time"

	"github.com/stealthrocket/coroutine"
)

type sleep time.Duration

// Sleep suspends the coroutine for the specified duration.
//
// If the program is built in durable mode, this causes the invoking function
// to capture the coroutine state and return to the scheduler.
func Sleep(d time.Duration) {
	start := time.Now()
	for {
		coroutine.Yield[any, any](sleep(d))
		// The remote scheduler's clock may not be in sync with the local clock,
		// but Sleep requires that we wait for at least the specified duration.
		//
		// We ensure that a time reading after returning from the function will
		// always result in a delay of at least the specified duration by either
		// doing a local sleep with the remaining delay is short, or by yielding
		// back to the scheduler if we need to sleep for a much longer time.
		delay := time.Since(start)
		if delay >= d {
			break
		}
		const maxSleepDuration = 100 * time.Millisecond
		if delay < maxSleepDuration {
			time.Sleep(maxSleepDuration - delay)
			break
		}
		d = time.Until(start.Add(d))
	}
}
