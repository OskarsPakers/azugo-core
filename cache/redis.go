// Copyright 2022 Azugo. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"azugo.io/core/instrumenter"

	"github.com/goccy/go-json"
	"github.com/valkey-io/valkey-go"
)

// conn is a Redis connection shared by all cache instances of a cache.
//
// The client is established on first use and a failed attempt is retried on the next one.
// The valkey client connects and completes the handshake when it is created, so creating
// it eagerly makes an unreachable cache fatal for the whole application: a cache that is
// down while the application starts (or an Istio sidecar that is not ready yet) would
// take the process down instead of failing the operations that need the cache.
type conn struct {
	create func() (valkey.Client, error)
	mu     sync.Mutex
	client valkey.Client
	closed bool
}

func newConn(create func() (valkey.Client, error)) *conn {
	return &conn{create: create}
}

// get returns the shared client, connecting if the connection is not established yet.
func (c *conn) get() (valkey.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, ErrCacheClosed
	}

	if c.client != nil {
		return c.client, nil
	}

	client, err := c.create()
	if err != nil {
		return nil, err
	}

	c.client = client

	return client, nil
}

// close releases the client. Any further operation returns ErrCacheClosed.
func (c *conn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		c.client.Close()
		c.client = nil
	}

	c.closed = true
}

type redisCache[T any] struct {
	con            *conn
	typ            Type
	name           string
	prefix         string
	ttl            time.Duration
	clientCacheTTL time.Duration
	loader         func(ctx context.Context, key string) (any, error)
	instrumenter   instrumenter.Instrumenter
}

func newRedisCache[T any](prefix string, con *conn, opts ...Option) Instance[T] {
	opt := newCacheOptions(opts...)

	keyPrefix := opt.KeyPrefix
	if keyPrefix != "" {
		keyPrefix += ":"
	}

	loader := opt.Loader
	if loader != nil {
		loader = func(ctx context.Context, key string) (any, error) {
			finish := instrumenter.ObserveKey(ctx, opt.Instrumenter, InstrumentationLoader, key)
			v, err := opt.Loader(ctx, key)
			finish(err)

			return v, err
		}
	}

	return &redisCache[T]{
		con:            con,
		typ:            opt.Type,
		name:           prefix,
		prefix:         keyPrefix + prefix + ":",
		ttl:            opt.TTL,
		clientCacheTTL: opt.ClientCacheTTL,
		loader:         loader,
		instrumenter:   opt.Instrumenter,
	}
}

func (c *redisCache[T]) observe(ctx context.Context, op, key string) func(error) {
	if c.instrumenter.Empty() {
		return instrumenter.NullFinish
	}

	return c.instrumenter.Observe(ctx, op, c.prefix+key, string(c.typ), c.name)
}

func parseRedisSentinelURL(urlStr string) (valkey.ClientOption, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return valkey.ClientOption{}, err
	}

	var tlsConfig *tls.Config

	switch u.Scheme {
	case "sentinel":
	case "sentinels":
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	default:
		return valkey.ClientOption{}, errors.New("redis sentinel URL must start with sentinel:// or sentinels:// scheme")
	}

	// Extract username if present
	username := ""
	if u.User != nil {
		username = u.User.Username()
	}

	masterName := strings.TrimPrefix(u.Path, "/")
	if masterName == "" {
		return valkey.ClientOption{}, errors.New("master name is required in sentinel URL path")
	}

	if u.Host == "" {
		return valkey.ClientOption{}, errors.New("sentinel addresses are required")
	}

	options := valkey.ClientOption{
		InitAddress: strings.Split(u.Host, ","),
		Username:    username,
		TLSConfig:   tlsConfig,
		Sentinel: valkey.SentinelOption{
			MasterSet: masterName,
			TLSConfig: tlsConfig,
		},
	}

	// Parse query parameters
	if u.RawQuery != "" {
		q := u.Query()

		if dbStr := q.Get("db"); dbStr != "" {
			db, err := strconv.Atoi(dbStr)
			if err != nil {
				return valkey.ClientOption{}, fmt.Errorf("invalid db value: %w", err)
			}

			options.SelectDB = db
		}

		if tlsConfig != nil && q.Get("skip_verify") == "true" {
			tlsConfig.InsecureSkipVerify = true
		}
	}

	return options, nil
}

func newValkeyClient(copt valkey.ClientOption, o *cacheOptions) (valkey.Client, error) {
	// If password is provided override provided in connection string.
	if len(o.ConnectionPassword) != 0 {
		copt.Password = o.ConnectionPassword
	}

	if !o.ClientCache {
		copt.DisableCache = true
	}

	if !copt.DisableCache && o.ClientCacheSize > 0 {
		copt.CacheSizeEachConn = o.ClientCacheSize
	}

	return valkey.NewClient(copt)
}

func newRedisClient(o *cacheOptions) (*conn, error) {
	copt, err := valkey.ParseURL(o.ConnectionString)
	if err != nil {
		return nil, err
	}

	return newConn(func() (valkey.Client, error) {
		return newValkeyClient(copt, o)
	}), nil
}

func newRedisSentinelClient(o *cacheOptions) (*conn, error) {
	copt, err := parseRedisSentinelURL(o.ConnectionString)
	if err != nil {
		return nil, err
	}

	return newConn(func() (valkey.Client, error) {
		return newValkeyClient(copt, o)
	}), nil
}

func (c *redisCache[T]) Get(ctx context.Context, key string, opts ...ItemOption[T]) (T, error) {
	val := new(T)

	finish := c.observe(ctx, InstrumentationGet, key)

	con, err := c.con.get()
	if err != nil {
		finish(err)

		return *val, err
	}

	var res valkey.ValkeyResult
	if c.clientCacheTTL > 0 {
		res = con.DoCache(ctx, con.B().Get().Key(c.prefix+key).Cache(), c.clientCacheTTL)
	} else {
		res = con.Do(ctx, con.B().Get().Key(c.prefix+key).Build())
	}

	if res.IsCacheHit() {
		c.observe(ctx, InstrumentationGetHit, key)(nil)
	}

	v, err := res.ToString()

	if valkey.IsValkeyNil(err) {
		if c.loader != nil {
			v, err := c.loader(ctx, key)
			if err != nil {
				finish(err)

				return *val, err
			}

			vv, ok := v.(T)
			if !ok {
				err = fmt.Errorf("invalid value from loader: %v", v)
				finish(err)

				return *val, err
			}

			if err := c.Set(ctx, key, vv, opts...); err != nil {
				finish(err)

				return *val, err
			}

			return vv, nil
		}

		return *val, nil
	}

	if err != nil {
		finish(err)

		return *val, err
	}

	if err := json.Unmarshal([]byte(v), val); err != nil {
		err = fmt.Errorf("invalid cache value: %w", err)
		finish(err)

		return *val, err
	}

	finish(nil)

	return *val, nil
}

func (c *redisCache[T]) Pop(ctx context.Context, key string) (T, error) {
	val := new(T)

	finishG := c.observe(ctx, InstrumentationGet, key)
	finishD := c.observe(ctx, InstrumentationDelete, key)

	con, err := c.con.get()
	if err != nil {
		finishD(err)
		finishG(err)

		return *val, err
	}

	v, err := con.Do(ctx, con.B().Getdel().Key(c.prefix+key).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		finishD(nil)
		finishG(nil)

		return *val, KeyNotFoundError{Key: key}
	}

	if err != nil {
		finishD(err)
		finishG(err)

		return *val, err
	}

	if err := json.Unmarshal([]byte(v), val); err != nil {
		err = fmt.Errorf("invalid cache value: %w", err)
		finishD(err)
		finishG(err)

		return *val, err
	}

	finishD(nil)
	finishG(nil)

	return *val, nil
}

func (c *redisCache[T]) Set(ctx context.Context, key string, value T, opts ...ItemOption[T]) error {
	finish := c.observe(ctx, InstrumentationSet, key)

	con, err := c.con.get()
	if err != nil {
		finish(err)

		return err
	}

	buf, err := json.Marshal(value)
	if err != nil {
		err = fmt.Errorf("invalid cache value: %w", err)
		finish(err)

		return err
	}

	opt := newItemOptions(opts...)

	ttl := c.ttl
	if opt.TTL != 0 {
		ttl = opt.TTL
	}

	cmd := con.B().Set().Key(c.prefix + key).Value(string(buf))

	var completed valkey.Completed
	if ttl > 0 {
		completed = cmd.Px(ttl).Build()
	} else {
		completed = cmd.Build()
	}

	if err := con.Do(ctx, completed).Error(); err != nil {
		finish(err)

		return err
	}

	finish(nil)

	return nil
}

func (c *redisCache[T]) Delete(ctx context.Context, key string) error {
	finish := c.observe(ctx, InstrumentationDelete, key)

	con, err := c.con.get()
	if err != nil {
		finish(err)

		return err
	}

	if err := con.Do(ctx, con.B().Del().Key(c.prefix+key).Build()).Error(); err != nil {
		finish(err)

		return err
	}

	finish(nil)

	return nil
}

func (c *redisCache[T]) Ping(ctx context.Context) error {
	con, err := c.con.get()
	if err != nil {
		return err
	}

	return con.Do(ctx, con.B().Ping().Build()).Error()
}

func (c *redisCache[T]) Close() error {
	c.con.close()

	return nil
}
