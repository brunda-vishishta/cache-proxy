package proxy

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/brunda-vishishta/cache-proxy/cache"
)

type ProxyObject struct {
	Origin    string
	Cache     map[string]*cache.CacheObject
	CacheSize int
	Order     []string
	Mutex     sync.RWMutex
}

func NewProxy(origin string, cacheSize int) *ProxyObject {
	return &ProxyObject{
		Origin:    origin,
		Cache:     make(map[string]*cache.CacheObject),
		CacheSize: cacheSize,
		Order:     []string{},
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

	p.Mutex.RLock()
	cachedResponse, found := p.Cache[cacheKey]
	if found {
		//update the order of cache keys to implement LRU eviction
		for i, key := range p.Order {
			if key == cacheKey {
				p.Order = append(p.Order[:i], p.Order[i+1:]...)
				break
			}
		}
		p.Order = append(p.Order, cacheKey)
		p.Mutex.RUnlock()
		RespondWithHeaders(w, *cachedResponse.Response, cachedResponse.ResponseBody, "HIT", cacheKey)
		return
	}

	fmt.Printf("Cache miss for %s\n", cacheKey)
	p.Mutex.RUnlock()
	fmt.Printf("Sending request to origin for %s\n", r.URL.Path)
	resp, err := http.Get(p.Origin + r.URL.Path)
	if err != nil {
		http.Error(w, "Error fetching from origin server", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Error reading response body", http.StatusInternalServerError)
		return
	}

	//check if cache size limit is reached and evict the least recently used entry
	if len(p.Cache) >= p.CacheSize {
		//remove the least recently used entry
		lruKey := p.Order[0]
		p.Order = p.Order[1:]
		delete(p.Cache, lruKey)
		fmt.Printf("Evicted least recently used cache entry: %s\n", lruKey)
	}

	//cache the response
	p.Mutex.Lock()
	p.Cache[cacheKey] = &cache.CacheObject{
		Response:     resp,
		ResponseBody: body,
		CreatedAt:    time.Now(),
	}
	p.Order = append(p.Order, cacheKey)
	p.Mutex.Unlock()

	RespondWithHeaders(w, *resp, body, "MISS", cacheKey)
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
