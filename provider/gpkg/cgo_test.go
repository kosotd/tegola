// +build cgo

package gpkg

import (
	"testing"

	"github.com/kosotd/tegola/dict"
	"github.com/kosotd/tegola/provider"
)

// This is a test to just see that the init function is doing something.
func TestNewProviderStartup(t *testing.T) {
	_, err := NewTileProvider(dict.Dict{})
	if err == provider.ErrUnsupported {
		t.Fatalf("supported, expected any but unsupported got %v", err)
	}
}
