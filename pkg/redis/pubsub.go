package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PubSub provides pub/sub operations for real-time messaging.
type PubSub struct {
	client *redis.Client
	ctx    context.Context
}

// NewPubSub creates a new pub/sub instance.
func NewPubSub(r *Redis) *PubSub {
	return &PubSub{
		client: r.Client,
		ctx:    context.Background(),
	}
}

// Message represents a pub/sub message.
type Message struct {
	Channel   string                 `json:"channel"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// Publish sends a message to a channel.
func (ps *PubSub) Publish(channel string, data map[string]interface{}) error {
	msg := Message{
		Channel:   channel,
		Data:      data,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("pubsub - Publish - json.Marshal: %w", err)
	}

	return ps.client.Publish(ps.ctx, channel, payload).Err()
}

// Subscribe creates a subscription to one or more channels.
func (ps *PubSub) Subscribe(channels ...string) *Subscription {
	pubsub := ps.client.Subscribe(ps.ctx, channels...)
	return &Subscription{
		pubsub:  pubsub,
		ctx:     ps.ctx,
		channels: channels,
	}
}

// PSubscribe creates a pattern-based subscription.
func (ps *PubSub) PSubscribe(patterns ...string) *Subscription {
	pubsub := ps.client.PSubscribe(ps.ctx, patterns...)
	return &Subscription{
		pubsub:   pubsub,
		ctx:      ps.ctx,
		patterns: patterns,
	}
}

// Subscription represents an active subscription.
type Subscription struct {
	pubsub   *redis.PubSub
	ctx      context.Context
	channels []string
	patterns []string
}

// Channel returns a channel for receiving messages.
func (s *Subscription) Channel() <-chan *Message {
	msgChan := make(chan *Message, 100)
	go func() {
		defer close(msgChan)
		ch := s.pubsub.Channel()
		for msg := range ch {
			var message Message
			if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
				// If unmarshaling fails, create a simple message
				message = Message{
					Channel:   msg.Channel,
					Data:      map[string]interface{}{"raw": msg.Payload},
					Timestamp: time.Now(),
				}
			}
			msgChan <- &message
		}
	}()
	return msgChan
}

// Close closes the subscription.
func (s *Subscription) Close() error {
	return s.pubsub.Close()
}

// Unsubscribe unsubscribes from channels.
func (s *Subscription) Unsubscribe(channels ...string) error {
	return s.pubsub.Unsubscribe(s.ctx, channels...)
}

// PUnsubscribe unsubscribes from patterns.
func (s *Subscription) PUnsubscribe(patterns ...string) error {
	return s.pubsub.PUnsubscribe(s.ctx, patterns...)
}
