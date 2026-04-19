package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/brunda-vishishta/cache-proxy/proxy"
)

func main() {
	port := flag.Int("port", 0, "port on which caching proxy server will run")
	origin := flag.String("origin", "", "URL of the server to which requests will be forwarded")
	clearCache := flag.Bool("clear-cache", false, "clear the cache if set to true")
	cacheSize := flag.Int("cache-size", 100, "maximum number of entries in the cache")
	flag.Parse()

	proxy := proxy.NewProxy("http://example.com", *cacheSize)

	if *clearCache {
		proxy.ClearCache()
		os.Exit(0)
	}

	if *origin != "" || *port != 0 {
		if *origin == "" {
			log.Fatal("origin URL is required")
		}
		proxy.Origin = *origin
		http.Handle("/", proxy)
		log.Printf("Starting caching proxy server on port %d forwarding to %s\n", *port, proxy.Origin)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
	} else {
		fmt.Println("both origin URL and port are required")
		flag.Usage()
	}

}
