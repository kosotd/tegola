package atlas

import (
	"reflect"
	"sort"
	"testing"

	"github.com/kosotd/tegola/cache"
)

func TestCheckCacheTypes(t *testing.T) {
	c := cache.Registered()
	exp := []string{"azblob", "file", "redis", "s3"}
	sort.Strings(exp)
	if !reflect.DeepEqual(c, exp) {
		t.Errorf("registered cachés, expected %v got %v", exp, c)
	}
}
