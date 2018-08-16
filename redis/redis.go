package redis

import (
	"time"

	leakybucket "github.com/DropLive/leakybucket"
	"github.com/garyburd/redigo/redis"
)

// Bucket object
type Bucket struct {
	name                string
	capacity, remaining uint
	reset               time.Time
	rate                time.Duration
	pool                *redis.Pool
}

// Capacity of Bucket
func (b *Bucket) Capacity() uint {
	return b.capacity
}

// Remaining space in the Bucket.
func (b *Bucket) Remaining() uint {
	return b.remaining
}

// Reset returns when the Bucket will be drained.
func (b *Bucket) Reset() time.Time {
	return b.reset
}

// State of bucket
func (b *Bucket) State() leakybucket.BucketState {
	return leakybucket.BucketState{Capacity: b.Capacity(), Remaining: b.Remaining(), Reset: b.Reset()}
}

var millisecond = int64(time.Millisecond)

func (b *Bucket) updateOldReset() error {
	if b.reset.Unix() > time.Now().Unix() {
		return nil
	}

	conn := b.pool.Get()
	defer conn.Close()

	ttl, err := conn.Do("PTTL", b.name)
	if err != nil {
		return err
	}
	b.reset = time.Now().Add(time.Duration(ttl.(int64) * millisecond))
	return nil
}

// Add to the Bucket.
func (b *Bucket) Add(amount uint) (leakybucket.BucketState, error) {
	conn := b.pool.Get()
	defer conn.Close()

	if count, err := redis.Uint64(conn.Do("GET", b.name)); err != nil {
		// handle the key not being set
		if err == redis.ErrNil {
			b.remaining = b.capacity
		} else {
			return b.State(), err
		}
	} else {
		b.remaining = b.capacity - min(uint(count), b.capacity)
	}

	if amount > b.remaining {
		b.updateOldReset()
		return b.State(), leakybucket.ErrorFull
	}

	// Go y u no have Milliseconds method? Why only Seconds and Nanoseconds?
	expiry := int(b.rate.Nanoseconds() / millisecond)

	count, err := redis.Uint64(conn.Do("INCRBY", b.name, amount))
	if err != nil {
		return b.State(), err
	} else if uint(count) == amount {
		if _, err := conn.Do("PEXPIRE", b.name, expiry); err != nil {
			return b.State(), err
		}
	}

	b.updateOldReset()

	// Ensure we can't overflow
	b.remaining = b.capacity - min(uint(count), b.capacity)
	return b.State(), nil
}

// Storage is a redis-based, non thread-safe leaky Bucket factory.
type Storage struct {
	pool *redis.Pool
}

// Create a Bucket.
func (s *Storage) Create(name string, capacity uint, rate time.Duration) (leakybucket.Bucket, error) {
	conn := s.pool.Get()
	defer conn.Close()

	if count, err := redis.Uint64(conn.Do("GET", name)); err != nil {
		if err != redis.ErrNil {
			return nil, err
		}
		// return a standard Bucket if key was not found
		return &Bucket{
			name:      name,
			capacity:  capacity,
			remaining: capacity,
			reset:     time.Now().Add(rate),
			rate:      rate,
			pool:      s.pool,
		}, nil
	} else if ttl, err := redis.Int64(conn.Do("PTTL", name)); err != nil {
		return nil, err
	} else {
		b := &Bucket{
			name:      name,
			capacity:  capacity,
			remaining: capacity - min(capacity, uint(count)),
			reset:     time.Now().Add(time.Duration(ttl * millisecond)),
			rate:      rate,
			pool:      s.pool,
		}
		return b, nil
	}
}

// NewBucket initializes the connection to redis.
func NewBucket(network, address string, password string) (*Storage, error) {
	s := &Storage{
		pool: redis.NewPool(func() (redis.Conn, error) {
			c, err := redis.Dial(network, address)
			if nil != err {
				return nil, err
			}

			if "" != password {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, nil
		}, 5)}
	// When using a connection pool, you only get connection errors while trying to send commands.
	// Try to PING so we can fail-fast in the case of invalid address.
	conn := s.pool.Get()
	defer conn.Close()
	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}
	return s, nil
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}
