package cache

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/gob"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cache/persistence"
	"github.com/gin-gonic/gin"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	noWritten = -1
)

var (
	PageCachePrefix = "gin.contrib.page.cache"
)

type responseCache struct {
	Status int
	Header http.Header
	Data   []byte
}

// RegisterResponseCacheGob registers the responseCache type with the encoding/gob package
func RegisterResponseCacheGob() {
	gob.Register(responseCache{})
}

type cachedWriter struct {
	gin.ResponseWriter
	status  int
	written bool
	store   persistence.CacheStore
	expire  time.Duration
	key     string
	size    int
	ctx     context.Context
}

// var _ gin.ResponseWriter = &cachedWriter{}

// CreateKey creates a package specific key for a given string
func CreateKey(u string, ext ...string) string {
	return urlEscape(PageCachePrefix, u+strings.Join(ext, ""))
}

func urlEscape(prefix string, u string) string {
	key := url.QueryEscape(u)
	if len(key) > 200 {
		h := sha1.New()
		_, _ = io.WriteString(h, u)
		key = string(h.Sum(nil))
	}
	var buffer bytes.Buffer
	buffer.WriteString(prefix)
	buffer.WriteString(":")
	buffer.WriteString(key)
	return buffer.String()
}

func newCachedWriter(ctx context.Context, store persistence.CacheStore, expire time.Duration, writer gin.ResponseWriter, key string) *cachedWriter {
	return &cachedWriter{ctx: ctx, ResponseWriter: writer, status: 0, written: false, store: store, expire: expire, key: key}
}

func (w *cachedWriter) WriteHeader(code int) {
	w.status = code
	w.written = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *cachedWriter) Write(data []byte) (int, error) {
	ret, err := w.ResponseWriter.Write(data)
	if err == nil {
		store := w.store
		// var cache responseCache
		// if err := store.Get(w.key, &cache); err == nil {
		// 	data = append(cache.Data, data...)
		// }

		// cache responses with a status code < 300
		if w.Status() < 300 {
			val := responseCache{
				w.Status(),
				w.Header(),
				data,
			}
			err = store.Set(w.ctx, w.key, val, w.expire)
			if err != nil {
				// need logger
			}
		}
	}
	return ret, nil
}

func (w *cachedWriter) WriteString(data string) (n int, err error) {
	// ret, err := w.ResponseWriter.WriteString(data)

	// w.WriteHeaderNow()
	if !w.Written() {
		w.size = 0
		w.ResponseWriter.WriteHeader(w.status)
	}
	n, err = io.WriteString(w.ResponseWriter, data)
	w.size += n

	// cache responses with a status code < 300
	if err == nil && w.Status() < 300 {
		store := w.store
		val := responseCache{
			w.Status(),
			w.Header(),
			[]byte(data),
		}
		store.Set(w.ctx, w.key, val, w.expire)
	}
	return n, err
}

func (w *cachedWriter) Status() int {
	return w.status
}

func (w *cachedWriter) Written() bool {
	return w.size != noWritten
}

// cachePage Decorator
func cachePage(store persistence.CacheStore, expire time.Duration, handle gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		var cache responseCache

		key := CreateKey(c.Request.URL.RequestURI())
		if c.Request.Method == "POST" {
			var bodyBytes []byte
			if c.Request.Body != nil {
				bodyBytes, _ = ioutil.ReadAll(c.Request.Body)
				c.Request.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			}
			key = CreateKey(c.Request.URL.RequestURI(), string(bodyBytes))
		}
		if err := store.Get(c.Request.Context(), key, &cache); err != nil {
			if err != persistence.ErrCacheMiss {
				log.Println(err.Error())
			}
			// replace writer
			c.Writer = newCachedWriter(c.Request.Context(), store, expire, c.Writer, key)
			handle(c)

			// Drop caches of aborted contexts
			if c.IsAborted() {
				_ = store.Delete(c.Request.Context(), key)
			}
		} else {
			c.Writer.WriteHeader(cache.Status)
			for k, vals := range cache.Header {
				for _, v := range vals {
					c.Writer.Header().Set(k, v)
				}
			}
			_, _ = c.Writer.Write(cache.Data)
		}
	}
}

// CachePageAtomic Decorator
func CachePageAtomic(store persistence.CacheStore, expire time.Duration, handle gin.HandlerFunc) gin.HandlerFunc {
	var m sync.Mutex
	p := cachePage(store, expire, handle)
	return func(c *gin.Context) {
		span, ctx := tracer.StartSpanFromContext(c.Request.Context(), "cache.CachePageAtomic")
		c.Request = c.Request.WithContext(ctx)
		defer span.Finish()
		m.Lock()
		defer m.Unlock()
		p(c)
	}
}
