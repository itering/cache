package cache

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-contrib/cache/persistence"
	"github.com/gin-gonic/gin"
)

// a simple in-memory store for the test
var testStore = persistence.NewMemoryStore(time.Minute)

// setup a Gin server with the problematic cache middleware
func setupRouterForTest() *gin.Engine {
	// Must set to release mode to enable sync.Pool for gin.Context
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	// Use your actual cache middleware here.
	// This uses CacheByRequestURI as an example.
	router.Use(CacheByRequestURI(testStore, 10*time.Second))
	router.Use(gin.Recovery())
	// This is our test endpoint.
	// We will make it artificially slow to create a window for the race condition.
	router.GET("/test", func(c *gin.Context) {
		// Simulate a slow database query or API call.
		// This happens inside the singleflight.DoChan func().
		time.Sleep(200 * time.Millisecond)
		c.String(http.StatusOK, "data from slow handler")
	})

	return router
}

// TestToReproduceRaceCondition is the test that will trigger the panic.
func TestToReproduceRaceCondition(t *testing.T) {
	router := setupRouterForTest()
	server := httptest.NewServer(router)
	defer server.Close()

	// Number of concurrent requests to send.
	// A high number increases the probability of triggering the context reuse.
	concurrency := 1000
	var wg sync.WaitGroup
	wg.Add(concurrency)

	t.Logf("Starting %d concurrent requests to %s/test", concurrency, server.URL)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			// Each goroutine uses its own client.
			// All requests hit the same URL to ensure they use the same singleflight key.
			resp, err := http.Get(server.URL + "/test")
			if err != nil {
				// We might see errors here if the server panics and closes the connection.
				// This is expected.
				t.Logf("Request failed (this might be expected if server panicked): %v", err)
				return
			}
			defer resp.Body.Close()
		}()
	}

	wg.Wait()
	t.Log("All requests have completed.")
}
