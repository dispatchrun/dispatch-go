package dispatch

import (
	"slices"
	"strings"
)

// Env sets the environment variables that a Dispatch endpoint
// or Client parses its default configuration from.
//
// It defaults to os.Environ().
func Env(env ...string) interface {
	DispatchOption
	ClientOption
} {
	return envOption(env)
}

type envOption []string

func (env envOption) configureDispatch(d *Dispatch) { d.env = slices.Clone(env) }
func (env envOption) configureClient(c *Client)     { c.env = slices.Clone(env) }

func getenv(env []string, name string) string {
	var value string
	for _, s := range env {
		n, v, ok := strings.Cut(s, "=")
		if ok && n == name {
			value = v
		}
	}
	return value
}
