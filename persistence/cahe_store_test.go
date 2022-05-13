package persistence

import (
	"context"
	"testing"
	"time"
)

type cacheFactory func(*testing.T, time.Duration) CacheStore

// Test typical cache interactions
func typicalGetSet(t *testing.T, newCache cacheFactory) {
	var err error
	ctx := context.TODO()
	cache := newCache(t, time.Hour)

	value := "foo"
	if err = cache.Set(ctx, "value", value, DEFAULT); err != nil {
		t.Errorf("Error setting a value: %s", err)
	}

	value = ""
	err = cache.Get(ctx, "value", &value)
	if err != nil {
		t.Errorf("Error getting a value: %s", err)
	}
	if value != "foo" {
		t.Errorf("Expected to get foo back, got %s", value)
	}
}

// Test the increment-decrement cases
func incrDecr(t *testing.T, newCache cacheFactory) {
	var err error
	ctx := context.TODO()
	cache := newCache(t, time.Hour)

	// Normal increment / decrement operation.
	if err = cache.Set(ctx, "int", 10, DEFAULT); err != nil {
		t.Errorf("Error setting int: %s", err)
	}
	newValue, err := cache.Increment(ctx, "int", 50)
	if err != nil {
		t.Errorf("Error incrementing int: %s", err)
	}
	var oriValue int
	_ = cache.Get(ctx, "int", &oriValue)
	if oriValue != 60 {
		t.Errorf("Expected 60, was %d", newValue)
	}

	if newValue, err = cache.Decrement(ctx, "int", 50); err != nil {
		t.Errorf("Error decrementing: %s", err)
	}
	_ = cache.Get(ctx, "int", &oriValue)
	if oriValue != 10 {
		t.Errorf("Expected 10, was %d", newValue)
	}
}

func expiration(t *testing.T, newCache cacheFactory) {
	ctx := context.TODO()
	// memcached does not support expiration times less than 1 second.
	var err error
	cache := newCache(t, time.Second)
	// Test Set w/ DEFAULT
	value := 10
	cache.Set(ctx, "int", value, DEFAULT)
	time.Sleep(2 * time.Second)
	err = cache.Get(ctx, "int", &value)
	if err != ErrCacheMiss {
		t.Errorf("Expected CacheMiss, but got: %s", err)
	}

	// Test Set w/ short time
	cache.Set(ctx, "int", value, time.Second)
	time.Sleep(2 * time.Second)
	err = cache.Get(ctx, "int", &value)
	if err != ErrCacheMiss {
		t.Errorf("Expected CacheMiss, but got: %s", err)
	}

	// Test Set w/ longer time.
	cache.Set(ctx, "int", value, time.Hour)
	time.Sleep(2 * time.Second)
	err = cache.Get(ctx, "int", &value)
	if err != nil {
		t.Errorf("Expected to get the value, but got: %s", err)
	}

	// Test Set w/ forever.
	cache.Set(ctx, "int", value, FOREVER)
	time.Sleep(2 * time.Second)
	err = cache.Get(ctx, "int", &value)
	if err != nil {
		t.Errorf("Expected to get the value, but got: %s", err)
	}
}

func emptyCache(t *testing.T, newCache cacheFactory) {
	var err error
	cache := newCache(t, time.Hour)

	ctx := context.TODO()
	err = cache.Get(ctx, "notexist", 0)
	if err == nil {
		t.Errorf("Error expected for non-existent key")
	}
	if err != ErrCacheMiss {
		t.Errorf("Expected ErrCacheMiss for non-existent key: %s", err)
	}

	_ = cache.Delete(ctx, "notexist")

	_, err = cache.Increment(ctx, "notexist", 1)
	if err != ErrCacheMiss {
		t.Errorf("Expected cache miss incrementing non-existent key: %s", err)
	}

	_, err = cache.Decrement(ctx, "notexist", 1)
	if err != ErrCacheMiss {
		t.Errorf("Expected cache miss decrementing non-existent key: %s", err)
	}
}

func testReplace(t *testing.T, newCache cacheFactory) {
	ctx := context.TODO()
	var err error
	cache := newCache(t, time.Hour)

	// Replace in an empty cache.
	if err = cache.Replace(ctx, "notexist", 1, FOREVER); err != ErrNotStored && err != ErrCacheMiss {
		t.Errorf("Replace in empty cache: expected ErrNotStored or ErrCacheMiss, got: %s", err)
	}

	// Set a value of 1, and replace it with 2
	if err = cache.Set(ctx, "int", 1, time.Second); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if err = cache.Replace(ctx, "int", 2, time.Second); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	var i int
	if err = cache.Get(ctx, "int", &i); err != nil {
		t.Errorf("Unexpected error getting a replaced item: %s", err)
	}
	if i != 2 {
		t.Errorf("Expected 2, got %d", i)
	}

	// Wait for it to expire and replace with 3 (unsuccessfully).
	time.Sleep(2 * time.Second)
	if err = cache.Replace(ctx, "int", 3, time.Second); err != ErrNotStored && err != ErrCacheMiss {
		t.Errorf("Expected ErrNotStored or ErrCacheMiss, got: %s", err)
	}
	if err = cache.Get(ctx, "int", &i); err != ErrCacheMiss {
		t.Errorf("Expected cache miss, got: %s", err)
	}
}

func testAdd(t *testing.T, newCache cacheFactory) {
	var err error
	ctx := context.TODO()
	cache := newCache(t, time.Hour)
	// Add to an empty cache.
	if err = cache.Add(ctx, "int", 1, time.Second); err != nil {
		t.Errorf("Unexpected error adding to empty cache: %s", err)
	}

	// Try to add again. (fail)
	if err = cache.Add(ctx, "int", 2, time.Second); err == nil {
		t.Errorf("Expected ErrNotStored adding dupe to cache: %s", err)
	}
	//
	// // Wait for it to expire, and add again.
	time.Sleep(2 * time.Second)
	if err = cache.Add(ctx, "int", 3, time.Second); err != nil {
		t.Errorf("Unexpected error adding to cache: %s", err)
	}
	//
	// Get and verify the value.
	var i int
	if err = cache.Get(ctx, "int", &i); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if i != 3 {
		t.Errorf("Expected 3, got: %d", i)
	}
}
