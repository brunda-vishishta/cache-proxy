package proxy

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/brunda-vishishta/cache-proxy/cache"
	"golang.org/x/sync/singleflight"
)

type ProxyObject struct {
	Origin    string
	Cache     map[string]*cache.CacheObject
	CacheSize int
	Order     []string
	Mutex     sync.RWMutex
	Group     singleflight.Group // prevents duplicate origin requests for the same key
	Semaphore chan struct{}      // limits max concurrent origin requests
}

func NewProxy(origin string, cacheSize int) *ProxyObject {
	return &ProxyObject{
		Origin:    origin,
		Cache:     make(map[string]*cache.CacheObject),
		CacheSize: cacheSize,
		Order:     []string{},
		Semaphore: make(chan struct{}, 10), // max 10 concurrent requests to origin
	}
}

func (p *ProxyObject) ClearCache() {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	p.Cache = make(map[string]*cache.CacheObject)
	fmt.Println("Cache cleared")
}

func (p *ProxyObject) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.Method + ":" + r.URL.String()

	// Check cache first with a read lock
	p.Mutex.RLock()
	cachedResponse, found := p.Cache[cacheKey]
	p.Mutex.RUnlock()

	if found {
		// Update LRU order with a write lock
		p.Mutex.Lock()
		for i, key := range p.Order {
			if key == cacheKey {
				p.Order = append(p.Order[:i], p.Order[i+1:]...)
				break
			}
		}
		p.Order = append(p.Order, cacheKey)
		p.Mutex.Unlock()

		RespondWithHeaders(w, *cachedResponse.Response, cachedResponse.ResponseBody, "HIT", cacheKey)
		return
	}

	fmt.Printf("Cache miss for %s\n", cacheKey)

	// Use singleflight to ensure only one request goes to origin for the same key.
	// All other concurrent requests for the same key will wait and share the result.
	result, err, _ := p.Group.Do(cacheKey, func() (interface{}, error) {
		// Acquire semaphore slot — blocks if 10 requests are already in flight
		p.Semaphore <- struct{}{}
		defer func() { <-p.Semaphore }() // release slot when done

		fmt.Printf("Sending request to origin for %s\n", r.URL.Path)
		resp, err := http.Get(p.Origin + r.URL.Path)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		// Write lock for cache update and LRU eviction
		p.Mutex.Lock()
		defer p.Mutex.Unlock()

		// Evict least recently used entry if cache is full
		if len(p.Cache) >= p.CacheSize {
			lruKey := p.Order[0]
			p.Order = p.Order[1:]
			delete(p.Cache, lruKey)
			fmt.Printf("Evicted least recently used cache entry: %s\n", lruKey)
		}

		// Store response in cache
		p.Cache[cacheKey] = &cache.CacheObject{
			Response:     resp,
			ResponseBody: body,
			CreatedAt:    time.Now(),
		}
		p.Order = append(p.Order, cacheKey)

		return &cache.CacheObject{
			Response:     resp,
			ResponseBody: body,
			CreatedAt:    time.Now(),
		}, nil
	})

	if err != nil {
		http.Error(w, "Error fetching from origin server", http.StatusInternalServerError)
		return
	}

	cacheObj := result.(*cache.CacheObject)
	RespondWithHeaders(w, *cacheObj.Response, cacheObj.ResponseBody, "MISS", cacheKey)
}

func RespondWithHeaders(w http.ResponseWriter, resp http.Response, body []byte, cacheStatus string, cacheKey string) {
	fmt.Printf("Cache %s for %s\n", cacheStatus, cacheKey)
	w.Header().Set("X-Cache", cacheStatus)
	w.WriteHeader(resp.StatusCode)
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.Write(body)
}
