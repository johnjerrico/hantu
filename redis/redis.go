package redis

import (
	"fmt"
	"time"

	r "github.com/gomodule/redigo/redis"
)

type Redis interface {
	HGetData(hash, field string) ([]byte, error)
	HSetData(hash, field string, data []byte, ttl ...int64) error
	GetData(key string) (data []byte, err error)
	SetData(key string, value []byte, ttl ...int64) (err error)
	Delete(key string) error
	HDelete(hash, field string) error
	Close() error
}

func New(host string, port string, ttl uint64) Redis {
	redisConnInfo := fmt.Sprintf("%s:%s", host, port)
	pool := initiatePool(redisConnInfo)
	return &redis{
		Pool: pool,
		ttl:  ttl,
	}
}

type redis struct {
	Pool *r.Pool
	ttl  uint64
}

func initiatePool(server string) *r.Pool {
	return &r.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,

		Dial: func() (r.Conn, error) {
			c, err := r.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},

		TestOnBorrow: func(c r.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
func (store *redis) Close() error {
	return store.Pool.Close()
}

func (store *redis) HGetData(hash, field string) ([]byte, error) {
	conn := store.Pool.Get()
	defer conn.Close()
	if tmp, err := r.Bytes(conn.Do("HGET", hash, field)); err != nil {
		return nil, fmt.Errorf("error get all key %s: %v", hash, err)
	} else {
		return tmp, nil
	}
}

func (store *redis) HSetData(hash, field string, data []byte, ttl ...int64) error {
	conn := store.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("HSet", hash, field, data); err != nil {
		return fmt.Errorf("error hset col : key %s : %s: %v : %v", hash, field, data, err)
	}
	timeToLive := int64(store.ttl)
	if len(ttl) > 0 {
		timeToLive = ttl[0]
	}

	if _, err := conn.Do("EXPIRE", hash, timeToLive); err != nil {
		return fmt.Errorf("error hset col : key %s : %s: %v : %v", hash, field, data, err)
	}
	return nil
}

func (store *redis) GetData(key string) (data []byte, err error) {
	conn := store.Pool.Get()
	defer conn.Close()

	data, err = r.Bytes(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, nil
}

func (store *redis) SetData(key string, value []byte, ttl ...int64) (err error) {
	conn := store.Pool.Get()
	defer conn.Close()

	timeToLive := int64(store.ttl)
	if len(ttl) > 0 {
		timeToLive = ttl[0]
	}

	_, err = conn.Do("SET", key, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}
	_, err = conn.Do("EXPIRE", key, timeToLive)
	if err != nil {
		return fmt.Errorf("error setting expiry time for key %s to %v: %v", key, timeToLive, err)
	}
	return nil
}

func (store *redis) Delete(key string) error {
	conn := store.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func (store *redis) HDelete(hash, field string) error {
	conn := store.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("HDEL", hash, field)
	return err
}
