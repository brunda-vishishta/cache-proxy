package cache

import (
	"net/http"
	"time"
)

type CacheObject struct {
	Response     *http.Response
	ResponseBody []byte
	CreatedAt    time.Time
}
