package proxy

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/brunda-vishishta/cache-proxy/cache"
	bolt "go.etcd.io/bbolt"
)

type ProxyObject struct {
	Origin    string
	Cache     map[string]*cache.CacheObject
	CacheSize int
	Order     []string
	Mutex     sync.RWMutex
	DB        *bolt.DB
}

func NewProxy(origin string, cacheSize int) *ProxyObject {
	//open a database
	db, err := bolt.Open("cache.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}

	//create bucket named cache
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("cache"))
		return err
	})

	proxy := &ProxyObject{
		Origin:    origin,
		Cache:     make(map[string]*cache.CacheObject),
		CacheSize: cacheSize,
		Order:     []string{},
		DB:        db,
	}

	//load existing cache from disk
	proxy.LoadCacheFromDisk()
	return proxy
}

func (p *ProxyObject) LoadCacheFromDisk() {
	p.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("cache"))
		if bucket == nil {
			return nil
		}

		bucket.ForEach(func(k, v []byte) error {
			if string(k) == "__LRU_ORDER__" {
				//deserialize LRU order
				buf := bytes.NewBuffer(v)
				decoder := gob.NewDecoder(buf)
				decoder.Decode(&p.Order)
				return nil
			}
			cacheObject, err := cache.DeserializeCacheObject(v)
			if err != nil {
				log.Println(err)
				return nil
			}
			p.Cache[string(k)] = cacheObject
			//p.Order = append(p.Order, string(k))
			return nil
		})
		return nil
	})
	fmt.Printf("Loaded %d entries from cache\n", len(p.Cache))

}

func (p *ProxyObject) ClearCache() {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	p.Cache = make(map[string]*cache.CacheObject)
	// Clear disk cache
	p.DB.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte("cache"))
		tx.CreateBucket([]byte("cache"))
		return nil
	})
	fmt.Println("Cache cleared (memory and disk)")
}

func (p *ProxyObject) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.Method + ":" + r.URL.String()

	p.Mutex.RLock() //read or write lock?
	cachedResponse, found := p.Cache[cacheKey]
	if found {
		// //update last accessed time
		// cachedResponse.LastAccessed = time.Now()
		// //persist updated timestamp
		// data, _ := cache.SerialzeCacheObject(cachedResponse)
		// p.DB.Update(func(tx *bolt.Tx) error {
		// 	bucket := tx.Bucket([]byte("cache"))
		// 	return bucket.Put([]byte(cacheKey), data)
		// })
		p.Mutex.RUnlock()
		p.Mutex.Lock()
		//update the order of cache keys (in memory) to implement LRU eviction
		for i, key := range p.Order {
			if key == cacheKey {
				p.Order = append(p.Order[:i], p.Order[i+1:]...)
				break
			}
		}
		p.Order = append(p.Order, cacheKey)
		err := p.PersistOrder() //unecessary to persist order on every hit?
		if err != nil {
			log.Printf("Failed to persist order: %v", err)
		}
		p.Mutex.Unlock()
		RespondWithHeaders(w, cachedResponse.StatusCode, cachedResponse.Headers, cachedResponse.ResponseBody, "HIT", cacheKey)
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
	p.Mutex.Lock() //check: should be inside if?
	if len(p.Cache) >= p.CacheSize {
		//remove the least recently used entry
		lruKey := p.Order[0]
		p.Order = p.Order[1:]
		delete(p.Cache, lruKey)
		//remove entry from disk
		p.DB.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("cache"))
			return bucket.Delete([]byte(lruKey))
		})
		fmt.Printf("Evicted least recently used cache entry: %s\n", lruKey)
	}

	//cache the response
	p.Cache[cacheKey] = &cache.CacheObject{
		StatusCode:   resp.StatusCode, // ✅ Simple int
		Status:       resp.Status,     // ✅ Simple string
		Headers:      resp.Header,     // ✅ Map of strings
		ResponseBody: body,
		CreatedAt:    time.Now(),
	}
	//persist new cache entry to disk
	data, err := cache.SerializeCacheObject(p.Cache[cacheKey])
	if err != nil {
		http.Error(w, "Error serializing cache object", http.StatusInternalServerError)
		return
	}
	err = p.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("cache"))
		return bucket.Put([]byte(cacheKey), data)
	})

	if err != nil {
		http.Error(w, "Error persisting cache object to DB", http.StatusInternalServerError)
		return
	}
	p.Order = append(p.Order, cacheKey)
	err = p.PersistOrder() //persist the lru order to DB
	if err != nil {
		log.Printf("Failed to persist order %v:", err)
	}
	p.Mutex.Unlock()

	RespondWithHeaders(w, resp.StatusCode, resp.Header, body, "MISS", cacheKey)
}

func RespondWithHeaders(w http.ResponseWriter, StatusCode int, Header map[string][]string, body []byte, cacheStatus string, cacheKey string) {
	fmt.Printf("Cache %s for %s\n", cacheStatus, cacheKey)
	w.Header().Set("X-Cache", cacheStatus)
	w.WriteHeader(StatusCode)
	for k, v := range Header {
		w.Header()[k] = v
	}
	w.Write(body)
}

func (p *ProxyObject) PersistOrder() error {
	return p.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("cache"))

		// Serialize the order slice
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(p.Order); err != nil {
			return err
		}

		// Store with special key
		return bucket.Put([]byte("__LRU_ORDER__"), buf.Bytes())
	})
}

func (p *ProxyObject) Close() error {
	if p.DB != nil {
		return p.DB.Close()
	}
	return nil
}
