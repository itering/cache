package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gin-contrib/cache/persistence"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/singleflight"
)

// Strategy the cache strategy
type Strategy struct {
	CacheKey string

	// CacheStore if nil, use default cache store instead
	CacheStore persistence.CacheStore

	// CacheDuration
	CacheDuration time.Duration
}

// GetCacheStrategyByRequest User can this function to design custom cache strategy by request.
// The first return value bool means whether this request should be cached.
// The second return value Strategy determine the special strategy by this request.
type GetCacheStrategyByRequest func(c *gin.Context) (bool, Strategy)

// Cache user must pass getCacheKey to describe the way to generate cache key
func Cache(
	defaultCacheStore persistence.CacheStore,
	defaultExpire time.Duration,
	opts ...Option,
) gin.HandlerFunc {
	cfg := newConfigByOpts(opts...)
	return cache(defaultCacheStore, defaultExpire, cfg)
}

func cache(
	defaultCacheStore persistence.CacheStore,
	defaultExpire time.Duration,
	cfg *Config,
) gin.HandlerFunc {
	if cfg.getCacheStrategyByRequest == nil {
		panic("cache strategy is nil")
	}

	sfGroup := singleflight.Group{}

	return func(c *gin.Context) {
		shouldCache, cacheStrategy := cfg.getCacheStrategyByRequest(c)
		if !shouldCache {
			c.Next()
			return
		}

		cacheKey := cacheStrategy.CacheKey

		if cfg.prefixKey != "" {
			cacheKey = cfg.prefixKey + cacheKey
		}

		// merge cfg
		cacheStore := defaultCacheStore
		if cacheStrategy.CacheStore != nil {
			cacheStore = cacheStrategy.CacheStore
		}

		cacheDuration := defaultExpire
		if cacheStrategy.CacheDuration > 0 {
			cacheDuration = cacheStrategy.CacheDuration
		}

		// read cache first
		{
			respCache := &ResponseCache{}
			err := cacheStore.Get(context.TODO(), cacheKey, &respCache)
			if err == nil {
				replyWithCache(c, cfg, respCache)
				cfg.hitCacheCallback(c)
				return
			}

			// if err != persistence.ErrCacheMiss {
			// }
		}

		// cache miss, then call the backend

		// use responseCacheWriter in order to record the response
		cacheWriter := &responseCacheWriter{ResponseWriter: c.Writer}
		c.Writer = cacheWriter

		inFlight := false
		rawRespCache, _, _ := sfGroup.Do(cacheKey, func() (interface{}, error) {
			if cfg.singleFlightForgetTimeout > 0 {
				forgetTimer := time.AfterFunc(cfg.singleFlightForgetTimeout, func() {
					sfGroup.Forget(cacheKey)
				})
				defer forgetTimer.Stop()
			}

			c.Next()

			inFlight = true

			respCache := &ResponseCache{}
			respCache.fillWithCacheWriter(cacheWriter)

			// only cache 2xx response
			if !c.IsAborted() && cacheWriter.Status() < 300 && cacheWriter.Status() >= 200 {
				_ = cacheStore.Set(context.TODO(), cacheKey, respCache, cacheDuration)
			}

			return respCache, nil
		})

		if !inFlight {
			replyWithCache(c, cfg, rawRespCache.(*ResponseCache))
			cfg.shareSingleFlightCallback(c)
		}
	}
}

// CacheByRequestURI a shortcut function for caching response by uri
func CacheByRequestURI(defaultCacheStore persistence.CacheStore, defaultExpire time.Duration, opts ...Option) gin.HandlerFunc {
	cfg := newConfigByOpts(opts...)

	cacheStrategy := func(c *gin.Context) (bool, Strategy) {

		var suffix string
		if c.Request.Method == "POST" {
			var bodyBytes []byte
			if c.Request.Body != nil {
				bodyBytes, _ = io.ReadAll(c.Request.Body)
				c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
			suffix = string(bodyBytes)
		}
		requestURI := c.Request.RequestURI + suffix
		newUri, err := getRequestUriIgnoreQueryOrder(requestURI)
		if err != nil {
			newUri = requestURI
		}

		return true, Strategy{CacheKey: newUri}
	}

	cfg.getCacheStrategyByRequest = cacheStrategy

	return cache(defaultCacheStore, defaultExpire, cfg)
}

func getRequestUriIgnoreQueryOrder(requestURI string) (string, error) {
	parsedUrl, err := url.ParseRequestURI(requestURI)
	if err != nil {
		return "", err
	}

	values := parsedUrl.Query()

	if len(values) == 0 {
		return requestURI, nil
	}

	queryKeys := make([]string, 0, len(values))
	for queryKey := range values {
		queryKeys = append(queryKeys, queryKey)
	}
	sort.Strings(queryKeys)

	queryVals := make([]string, 0, len(values))
	for _, queryKey := range queryKeys {
		sort.Strings(values[queryKey])
		for _, val := range values[queryKey] {
			queryVals = append(queryVals, queryKey+"="+val)
		}
	}
	return parsedUrl.Path + "?" + strings.Join(queryVals, "&"), nil
}

func init() {
	gob.Register(&ResponseCache{})
}

// ResponseCache record the http response cache
type ResponseCache struct {
	Status int
	Header http.Header
	Data   []byte
}

func (c *ResponseCache) fillWithCacheWriter(cacheWriter *responseCacheWriter) {
	c.Status = cacheWriter.Status()
	c.Data = cacheWriter.body.Bytes()
	c.Header = cacheWriter.Header().Clone()
}

// responseCacheWriter
type responseCacheWriter struct {
	gin.ResponseWriter
	body bytes.Buffer
}

func (w *responseCacheWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

func (w *responseCacheWriter) WriteString(s string) (int, error) {
	w.body.WriteString(s)
	return w.ResponseWriter.WriteString(s)
}

func replyWithCache(
	c *gin.Context,
	cfg *Config,
	respCache *ResponseCache,
) {
	cfg.beforeReplyWithCacheCallback(c, respCache)

	c.Writer.WriteHeader(respCache.Status)

	for key, values := range respCache.Header {
		for _, val := range values {
			c.Writer.Header().Set(key, val)
		}
	}

	if _, err := c.Writer.Write(respCache.Data); err != nil {
		cfg.logger.Errorf("write response error: %s", err)
	}
	c.Abort()
}
