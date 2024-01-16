//go:build durable

package dispatch

import (
	time "time"
	coroutine "github.com/stealthrocket/coroutine"
)
import _types "github.com/stealthrocket/coroutine/types"

type sleep time.Duration

// Sleep suspends the coroutine for the specified duration.
//
// If the program is built in durable mode, this causes the invoking function
// to capture the coroutine state and return to the scheduler.
//
//go:noinline
func Sleep(_fn0 time.Duration) {
	_c := coroutine.LoadContext[any, any]()
	var _f0 *struct {
		IP int
		X0 time.Duration
		X1 time.Time
		X2 time.Duration
		X3 time.Time
		X4 time.Duration
	} = coroutine.Push[struct {
		IP int
		X0 time.Duration
		X1 time.Time
		X2 time.Duration
		X3 time.Time
		X4 time.Duration
	}](&_c.Stack)

	const _o0 = 100 * time.Millisecond
	if _f0.IP == 0 {
		*_f0 = struct {
			IP int
			X0 time.Duration
			X1 time.Time
			X2 time.Duration
			X3 time.Time
			X4 time.Duration
		}{X0: _fn0}
	}
	defer func() {
		if !_c.Unwinding() {
			coroutine.Pop(&_c.Stack)
		}
	}()
	switch {
	case _f0.IP < 2:
		_f0.X1 = time.Now()
		_f0.IP = 2
		fallthrough
	case _f0.IP < 10:
	_l0:
		for ; ; _f0.IP = 2 {
			switch {
			case _f0.IP < 3:

				coroutine.Yield[any, any](sleep(_f0.X0))
				_f0.IP = 3
				fallthrough
			case _f0.IP < 4:
				_f0.X2 = time.Since(_f0.X1)
				_f0.IP = 4
				fallthrough
			case _f0.IP < 5:
				if _f0.X2 >= _f0.X0 {
					break _l0
				}
				_f0.IP = 5
				fallthrough
			case _f0.IP < 7:
				if _f0.X2 < _o0 {
					switch {
					case _f0.IP < 6:
						time.Sleep(_o0 - _f0.X2)
						_f0.IP = 6
						fallthrough
					case _f0.IP < 7:
						break _l0
					}
				}
				_f0.IP = 7
				fallthrough
			case _f0.IP < 8:
				_f0.X3 = _f0.X1.
					Add(_f0.X0)
				_f0.IP = 8
				fallthrough
			case _f0.IP < 9:
				_f0.X4 = time.Until(_f0.X3)
				_f0.IP = 9
				fallthrough
			case _f0.IP < 10:
				_f0.X0 = _f0.X4
			}
		}
	}
}
func init() {
	_types.RegisterFunc[func(_fn0 time.Duration)]("github.com/stealthrocket/dispatch/sdk/dispatch-go.Sleep")
}
