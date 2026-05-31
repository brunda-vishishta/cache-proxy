package cache

import (
	"time"
)

type CacheObject struct {
	//Response     *http.Response
	StatusCode   int
	Status       string
	Headers      map[string][]string
	ResponseBody []byte
	CreatedAt    time.Time
}
