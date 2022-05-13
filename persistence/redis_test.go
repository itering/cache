package persistence

import (
	"net"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	redigotrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/gomodule/redigo"
)

// These tests require redis server running on localhost:6379 (the default)
const redisTestServer = "localhost:6379"

var newRedisStore = func(t *testing.T, defaultExpiration time.Duration) CacheStore {
	c, err := net.Dial("tcp", redisTestServer)
	if err == nil {
		c.Write([]byte("flush_all\r\n"))
		c.Close()
		var pool = &redis.Pool{MaxIdle: 5, IdleTimeout: 240 * time.Second, Dial: func() (redis.Conn, error) {
			// the redis protocol should probably be made sett-able
			c, err := redigotrace.Dial("tcp", redisTestServer)
			if err != nil {
				return nil, err
			}
			return c, err
		}}
		redisCache := NewRedisCache(pool, defaultExpiration, "")
		return redisCache
	}
	t.Errorf("couldn't connect to redis on %s", redisTestServer)
	t.FailNow()
	return nil
}

func TestRedisCache_TypicalGetSet(t *testing.T) {
	typicalGetSet(t, newRedisStore)
}

func TestRedisCache_IncrDecr(t *testing.T) {
	incrDecr(t, newRedisStore)
}

func TestRedisCache_Expiration(t *testing.T) {
	expiration(t, newRedisStore)
}

func TestRedisCache_EmptyCache(t *testing.T) {
	emptyCache(t, newRedisStore)
}

func TestRedisCache_Replace(t *testing.T) {
	testReplace(t, newRedisStore)
}

func TestRedisCache_Add(t *testing.T) {
	testAdd(t, newRedisStore)
}
