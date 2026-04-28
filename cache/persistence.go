package cache

import (
	"bytes"
	"encoding/gob"
)

// convert the cache object to a byte array using gob encoding
func SerializeCacheObject(cacheObject *CacheObject) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(cacheObject)
	return buf.Bytes(), err
}

// convert the byte array back to a cache object using gob decoding
func DeserializeCacheObject(data []byte) (*CacheObject, error) {
	var cacheObject CacheObject
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(&cacheObject)
	return &cacheObject, err
}
