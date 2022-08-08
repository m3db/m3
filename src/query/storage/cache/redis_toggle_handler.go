package cache

import (
	"fmt"
	"net/http"
	"strconv"
)

const (
	ToggleRedisURL     = "/api/v1/redis/toggle"
	ToggleRedisMethods = "POST"
)

var EnableCache = true

type RedisToggleHandler struct{}

func (h *RedisToggleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	toggleVal := r.FormValue("toggle")
	toggle, err := strconv.ParseBool(toggleVal)
	if err != nil {
		fmt.Fprintf(w, "Couldn't parse request")
		return
	}
	fmt.Fprintf(w, "OK: Toggling from %t to %t", EnableCache, toggle)
	EnableCache = toggle
}
