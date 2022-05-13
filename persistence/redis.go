package persistence

import (
	"context"
	"fmt"
	"time"

	"github.com/gin-contrib/cache/utils"
	"github.com/gomodule/redigo/redis"
)

// RedisStore represents the cache with redis persistence
type RedisStore struct {
	pool              *redis.Pool
	defaultExpiration time.Duration
	prefix            string
	ctx               context.Context
}

// NewRedisCache returns a RedisStore
// until redigo supports sharding/clustering, only one host will be in hostList
func NewRedisCache(pool *redis.Pool, defaultExpiration time.Duration, prefix string) *RedisStore {
	return &RedisStore{pool, defaultExpiration, prefix, context.TODO()}
}

// Set (see CacheStore interface)
func (c *RedisStore) Set(ctx context.Context, key string, value interface{}, expires time.Duration) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return c.invoke(ctx, conn.Do, c.KeyWithPrefix(key), value, expires)
}

// Add (see CacheStore interface)
func (c *RedisStore) Add(ctx context.Context, key string, value interface{}, expires time.Duration) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if exists(ctx, conn, c.KeyWithPrefix(key)) {
		return ErrNotStored
	}
	return c.invoke(ctx, conn.Do, c.KeyWithPrefix(key), value, expires)
}

// Replace (see CacheStore interface)
func (c *RedisStore) Replace(ctx context.Context, key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	if !exists(ctx, conn, c.KeyWithPrefix(key)) {
		return ErrNotStored
	}
	err := c.invoke(ctx, conn.Do, c.KeyWithPrefix(key), value, expires)
	if value == nil {
		return ErrNotStored
	}
	return err
}

// Get (see CacheStore interface)
func (c *RedisStore) Get(ctx context.Context, key string, ptrValue interface{}) error {
	conn, err := c.pool.GetContext(c.ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	raw, err := conn.Do("GET", c.KeyWithPrefix(key), ctx)
	if raw == nil {
		return ErrCacheMiss
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return utils.Deserialize(item, ptrValue)
}

func exists(ctx context.Context, conn redis.Conn, key string) bool {
	retval, _ := redis.Bool(conn.Do("EXISTS", key, ctx))
	return retval
}

// Delete (see CacheStore interface)
func (c *RedisStore) Delete(ctx context.Context, key string) error {
	conn, err := c.pool.GetContext(c.ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if !exists(ctx, conn, c.KeyWithPrefix(key)) {
		return ErrCacheMiss
	}
	_, err = conn.Do("DEL", c.KeyWithPrefix(key), ctx)
	return err
}

// Increment (see CacheStore interface)
func (c *RedisStore) Increment(ctx context.Context, key string, delta uint64) (uint64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that. Since we need to do increment
	// ourselves instead of natively via INCRBY (redis doesn't support wrapping), we get the value
	// and do the exists check this way to minimize calls to Redis
	val, err := conn.Do("GET", c.KeyWithPrefix(key), ctx)
	if val == nil {
		return 0, ErrCacheMiss
	}
	if err == nil {
		currentVal, err := redis.Int64(val, nil)
		if err != nil {
			return 0, err
		}
		sum := currentVal + int64(delta)
		_, err = conn.Do("SET", c.KeyWithPrefix(key), sum, ctx)
		if err != nil {
			return 0, err
		}
		return uint64(sum), nil
	}

	return 0, err
}

// Decrement (see CacheStore interface)
func (c *RedisStore) Decrement(ctx context.Context, key string, delta uint64) (newValue uint64, err error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that, hence the exists call
	if !exists(ctx, conn, c.KeyWithPrefix(key)) {
		return 0, ErrCacheMiss
	}
	// Decrement contract says you can only go to 0
	// so we go fetch the value and if the delta is greater than the amount,
	// 0 out the value
	currentVal, err := redis.Int64(conn.Do("GET", c.KeyWithPrefix(key), ctx))
	if err == nil && delta > uint64(currentVal) {
		tempint, err := redis.Int64(conn.Do("DECRBY", c.KeyWithPrefix(key), currentVal, ctx))
		return uint64(tempint), err
	}
	tempint, err := redis.Int64(conn.Do("DECRBY", c.KeyWithPrefix(key), delta, ctx))
	return uint64(tempint), err
}

func (c *RedisStore) invoke(ctx context.Context, f func(string, ...interface{}) (interface{}, error),
	key string, value interface{}, expires time.Duration) error {

	switch expires {
	case DEFAULT:
		expires = c.defaultExpiration
	case FOREVER:
		expires = time.Duration(0)
	}

	b, err := utils.Serialize(value)
	if err != nil {
		return err
	}

	if expires > 0 {
		_, err := f("SETEX", key, int32(expires/time.Second), b, ctx)
		return err
	}

	_, err = f("SET", key, b, ctx)
	return err

}

func (c *RedisStore) KeyWithPrefix(key string) string {
	if c.prefix != "" {
		return fmt.Sprintf("%s:%s", c.prefix, key)
	}
	return key
}
