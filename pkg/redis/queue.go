package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue provides message queue operations for background jobs.
type Queue struct {
	client *redis.Client
	ctx    context.Context
}

// NewQueue creates a new queue instance.
func NewQueue(r *Redis) *Queue {
	return &Queue{
		client: r.Client,
		ctx:    context.Background(),
	}
}

// Job represents a background job.
type Job struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	CreatedAt time.Time              `json:"created_at"`
	Retries   int                    `json:"retries"`
}

// Push adds a job to the queue.
func (q *Queue) Push(queueName string, job *Job) error {
	if job.ID == "" {
		job.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue - Push - json.Marshal: %w", err)
	}

	return q.client.LPush(q.ctx, queueName, data).Err()
}

// Pop removes and returns a job from the queue (blocking).
func (q *Queue) Pop(queueName string, timeout time.Duration) (*Job, error) {
	result, err := q.client.BRPop(q.ctx, timeout, queueName).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Timeout, no job available
		}
		return nil, fmt.Errorf("queue - Pop - client.BRPop: %w", err)
	}

	if len(result) < 2 {
		return nil, fmt.Errorf("queue - Pop - invalid result format")
	}

	var job Job
	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, fmt.Errorf("queue - Pop - json.Unmarshal: %w", err)
	}

	return &job, nil
}

// PopNonBlocking removes and returns a job from the queue (non-blocking).
func (q *Queue) PopNonBlocking(queueName string) (*Job, error) {
	result, err := q.client.RPop(q.ctx, queueName).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No job available
		}
		return nil, fmt.Errorf("queue - PopNonBlocking - client.RPop: %w", err)
	}

	var job Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, fmt.Errorf("queue - PopNonBlocking - json.Unmarshal: %w", err)
	}

	return &job, nil
}

// Size returns the number of jobs in the queue.
func (q *Queue) Size(queueName string) (int64, error) {
	return q.client.LLen(q.ctx, queueName).Result()
}

// Clear removes all jobs from the queue.
func (q *Queue) Clear(queueName string) error {
	return q.client.Del(q.ctx, queueName).Err()
}

// PushToDelayedQueue adds a job to a delayed queue (using sorted set).
func (q *Queue) PushToDelayedQueue(queueName string, job *Job, delay time.Duration) error {
	if job.ID == "" {
		job.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue - PushToDelayedQueue - json.Marshal: %w", err)
	}

	score := float64(time.Now().Add(delay).Unix())
	return q.client.ZAdd(q.ctx, queueName+":delayed", redis.Z{
		Score:  score,
		Member: data,
	}).Err()
}

// ProcessDelayedQueue moves ready jobs from delayed queue to main queue.
func (q *Queue) ProcessDelayedQueue(queueName string) (int, error) {
	now := time.Now().Unix()
	delayedQueueName := queueName + ":delayed"

	// Get jobs that are ready (score <= now)
	vals, err := q.client.ZRangeByScore(q.ctx, delayedQueueName, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("queue - ProcessDelayedQueue - client.ZRangeByScore: %w", err)
	}

	if len(vals) == 0 {
		return 0, nil
	}

	// Move jobs to main queue and remove from delayed queue
	pipe := q.client.Pipeline()
	for _, val := range vals {
		pipe.LPush(q.ctx, queueName, val)
		pipe.ZRem(q.ctx, delayedQueueName, val)
	}

	_, err = pipe.Exec(q.ctx)
	if err != nil {
		return 0, fmt.Errorf("queue - ProcessDelayedQueue - pipe.Exec: %w", err)
	}

	return len(vals), nil
}
