//go:build !durable

package dispatchcoro

import (
	"github.com/dispatchrun/coroutine"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Coroutine is the flavour of coroutine supported by Dispatch and the SDK.
type Coroutine = coroutine.Coroutine[dispatchproto.Response, dispatchproto.Request]

// Yield yields control to Dispatch.
//
// The coroutine is suspended while the Response is sent to Dispatch.
// If the Response carries a directive to perform work, Dispatch will
// send the results back in a Request and resume execution from this
// point.
func Yield(res dispatchproto.Response) dispatchproto.Request {
	return coroutine.Yield[dispatchproto.Response, dispatchproto.Request](res)
}
