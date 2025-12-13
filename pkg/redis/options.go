package redis

import "time"

// Option -.
type Option func(*Redis)

// PoolSize -.
func PoolSize(size int) Option {
	return func(r *Redis) {
		r.poolSize = size
	}
}

// DB -.
func DB(db int) Option {
	return func(r *Redis) {
		r.db = db
	}
}

// ConnAttempts -.
func ConnAttempts(attempts int) Option {
	return func(r *Redis) {
		r.connAttempts = attempts
	}
}

// ConnTimeout -.
func ConnTimeout(timeout time.Duration) Option {
	return func(r *Redis) {
		r.connTimeout = timeout
	}
}
