// Package redis implements Redis connection and operations.
package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	_defaultPoolSize   = 10
	_defaultDB         = 0
	_defaultConnAttempts = 10
	_defaultConnTimeout  = time.Second
)

// Redis -.
type Redis struct {
	poolSize    int
	db          int
	connAttempts int
	connTimeout  time.Duration

	Client *redis.Client
}

// New -.
func New(url string, opts ...Option) (*Redis, error) {
	r := &Redis{
		poolSize:    _defaultPoolSize,
		db:          _defaultDB,
		connAttempts: _defaultConnAttempts,
		connTimeout:  _defaultConnTimeout,
	}

	// Custom options
	for _, opt := range opts {
		opt(r)
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis - New - redis.ParseURL: %w", err)
	}

	opt.PoolSize = r.poolSize
	opt.DB = r.db

	r.Client = redis.NewClient(opt)

	// Test connection with retries
	ctx, cancel := context.WithTimeout(context.Background(), r.connTimeout)
	defer cancel()

	for r.connAttempts > 0 {
		err = r.Client.Ping(ctx).Err()
		if err == nil {
			break
		}

		log.Printf("Redis is trying to connect, attempts left: %d", r.connAttempts)

		time.Sleep(r.connTimeout)

		r.connAttempts--
	}

	if err != nil {
		return nil, fmt.Errorf("redis - New - connAttempts == 0: %w", err)
	}

	return r, nil
}

// Close -.
func (r *Redis) Close() error {
	if r.Client != nil {
		return r.Client.Close()
	}
	return nil
}
