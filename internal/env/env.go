package env

import "strings"

// Get gets an environment variable value from a set of environment variables.
func Get(env []string, name string) string {
	var value string
	for _, s := range env {
		n, v, ok := strings.Cut(s, "=")
		if ok && n == name {
			value = v
		}
	}
	return value
}
