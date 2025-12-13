package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache provides caching operations.
type Cache struct {
	client *redis.Client
	ctx    context.Context
}

// NewCache creates a new cache instance.
func NewCache(r *Redis) *Cache {
	return &Cache{
		client: r.Client,
		ctx:    context.Background(),
	}
}

// Set stores a value in cache with expiration.
func (c *Cache) Set(key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cache - Set - json.Marshal: %w", err)
	}

	return c.client.Set(c.ctx, key, data, expiration).Err()
}

// Get retrieves a value from cache.
func (c *Cache) Get(key string, dest interface{}) error {
	data, err := c.client.Get(c.ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("cache - Get - key not found: %s", key)
		}
		return fmt.Errorf("cache - Get - client.Get: %w", err)
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("cache - Get - json.Unmarshal: %w", err)
	}

	return nil
}

// Delete removes a key from cache.
func (c *Cache) Delete(key string) error {
	return c.client.Del(c.ctx, key).Err()
}

// DeletePattern removes all keys matching a pattern.
func (c *Cache) DeletePattern(pattern string) error {
	iter := c.client.Scan(c.ctx, 0, pattern, 0).Iterator()
	for iter.Next(c.ctx) {
		if err := c.client.Del(c.ctx, iter.Val()).Err(); err != nil {
			return fmt.Errorf("cache - DeletePattern - client.Del: %w", err)
		}
	}
	return iter.Err()
}

// Exists checks if a key exists in cache.
func (c *Cache) Exists(key string) (bool, error) {
	count, err := c.client.Exists(c.ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("cache - Exists - client.Exists: %w", err)
	}
	return count > 0, nil
}

// SetNX sets a key only if it doesn't exist (atomic operation).
func (c *Cache) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("cache - SetNX - json.Marshal: %w", err)
	}

	return c.client.SetNX(c.ctx, key, data, expiration).Result()
}

// Increment increments a numeric value in cache.
func (c *Cache) Increment(key string) (int64, error) {
	return c.client.Incr(c.ctx, key).Result()
}

// IncrementBy increments a numeric value by a specific amount.
func (c *Cache) IncrementBy(key string, value int64) (int64, error) {
	return c.client.IncrBy(c.ctx, key, value).Result()
}
