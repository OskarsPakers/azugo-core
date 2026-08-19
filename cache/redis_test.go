package cache

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"azugo.io/core/instrumenter"

	"github.com/go-quicktest/qt"
)

func getRedisConnStr() string {
	return os.Getenv("REDIS_CONNSTR")
}

func TestParseRedisSentinelURL(t *testing.T) {
	opt, err := parseRedisSentinelURL("sentinel://user@s1:26379,s2:26379/mymaster?db=2")
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.DeepEquals(opt.InitAddress, []string{"s1:26379", "s2:26379"}))
	qt.Check(t, qt.Equals(opt.Username, "user"))
	qt.Check(t, qt.Equals(opt.Sentinel.MasterSet, "mymaster"))
	qt.Check(t, qt.Equals(opt.SelectDB, 2))
	qt.Check(t, qt.IsNil(opt.TLSConfig))
	qt.Check(t, qt.IsNil(opt.Sentinel.TLSConfig))
}

func TestParseRedisSentinelURLTLS(t *testing.T) {
	opt, err := parseRedisSentinelURL("sentinels://s1:26379/mymaster")
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNotNil(opt.TLSConfig))
	qt.Check(t, qt.IsFalse(opt.TLSConfig.InsecureSkipVerify))
	qt.Check(t, qt.Equals(opt.Sentinel.TLSConfig, opt.TLSConfig))

	opt, err = parseRedisSentinelURL("sentinels://s1:26379/mymaster?skip_verify=true")
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNotNil(opt.TLSConfig))
	qt.Check(t, qt.IsTrue(opt.TLSConfig.InsecureSkipVerify))

	// skip_verify without TLS scheme is ignored.
	opt, err = parseRedisSentinelURL("sentinel://s1:26379/mymaster?skip_verify=true")
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.IsNil(opt.TLSConfig))
}

func TestParseRedisSentinelURLErrors(t *testing.T) {
	_, err := parseRedisSentinelURL("redis://s1:26379/mymaster")
	qt.Check(t, qt.IsNotNil(err))

	_, err = parseRedisSentinelURL("sentinel://s1:26379")
	qt.Check(t, qt.IsNotNil(err))

	_, err = parseRedisSentinelURL("sentinel:///mymaster")
	qt.Check(t, qt.IsNotNil(err))

	_, err = parseRedisSentinelURL("sentinel://s1:26379/mymaster?db=abc")
	qt.Check(t, qt.IsNotNil(err))
}

func TestRedisCacheGetSet(t *testing.T) {
	cs := getRedisConnStr()
	if cs == "" {
		t.Skip("REDIS_CONNSTR is not set")
	}
	c := New(RedisCache, KeyPrefix("prefix"), ConnectionString(cs))
	err := c.Start(context.TODO())
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()

	i, err := Create[string](c, "test")
	qt.Assert(t, qt.IsNil(err))

	err = i.Set(context.TODO(), "key1", "value")
	qt.Check(t, qt.IsNil(err))

	val, err := i.Get(context.TODO(), "key1")
	qt.Check(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, "value"))
}

func TestRedisCachePop(t *testing.T) {
	cs := getRedisConnStr()
	if cs == "" {
		t.Skip("REDIS_CONNSTR is not set")
	}
	c := New(RedisCache, ConnectionString(cs))
	err := c.Start(context.TODO())
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()

	i, err := Create[string](c, "test")
	qt.Assert(t, qt.IsNil(err))

	err = i.Set(context.TODO(), "key2", "value")
	qt.Check(t, qt.IsNil(err))

	val, err := i.Pop(context.TODO(), "key2")
	qt.Check(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, "value"))

	val, err = i.Pop(context.TODO(), "key2")
	qt.Check(t, qt.IsNotNil(err))
	qt.Check(t, qt.Equals(val, ""))
}

func TestRedisCacheDelete(t *testing.T) {
	cs := getRedisConnStr()
	if cs == "" {
		t.Skip("REDIS_CONNSTR is not set")
	}
	c := New(RedisCache, ConnectionString(cs))
	err := c.Start(context.TODO())
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()

	i, err := Create[string](c, "test")
	qt.Assert(t, qt.IsNil(err))

	err = i.Set(context.TODO(), "key3", "value")
	qt.Check(t, qt.IsNil(err))

	err = i.Delete(context.TODO(), "key3")
	qt.Check(t, qt.IsNil(err))

	val, err := i.Get(context.TODO(), "key3")
	qt.Check(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, ""))
}

func TestRedisCacheExpire(t *testing.T) {
	cs := getRedisConnStr()
	if cs == "" {
		t.Skip("REDIS_CONNSTR is not set")
	}
	c := New(RedisCache, ConnectionString(cs))
	err := c.Start(context.TODO())
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()

	i, err := Create[string](c, "test", DefaultTTL(100*time.Millisecond))
	qt.Assert(t, qt.IsNil(err))

	err = i.Set(context.TODO(), "key4", "value")
	qt.Check(t, qt.IsNil(err))

	time.Sleep(150 * time.Millisecond)

	val, err := i.Get(context.TODO(), "key4")
	qt.Check(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, ""))
}

func TestRedisCacheUnreachable(t *testing.T) {
	// An unreachable cache must fail the operations that need it, not the whole
	// application: the connection is established on first use and retried later.
	c := New(RedisCache, ConnectionString("redis://127.0.0.1:1"))
	qt.Assert(t, qt.IsNil(c.Start(context.TODO())))

	i, err := Create[string](c, "test")
	qt.Assert(t, qt.IsNil(err))

	qt.Check(t, qt.IsNotNil(i.Set(context.TODO(), "key", "value")))

	_, err = i.Get(context.TODO(), "key")
	qt.Check(t, qt.IsNotNil(err))
	qt.Check(t, qt.IsNotNil(c.Ping(context.TODO())))

	// The connection error is reported to the caller instead of a nil client.
	_, err = c.Connection()
	qt.Check(t, qt.IsNotNil(err))

	c.Close()

	// A closed cache is reported as closed, not as unreachable.
	qt.Check(t, qt.IsTrue(errors.Is(i.Set(context.TODO(), "key", "value"), ErrCacheClosed)))

	_, err = c.Connection()
	qt.Check(t, qt.IsTrue(errors.Is(err, ErrCacheClosed)))
}

func TestConnectionNotStarted(t *testing.T) {
	c := New(RedisCache, ConnectionString("redis://127.0.0.1:1"))

	_, err := c.Connection()
	qt.Check(t, qt.IsTrue(errors.Is(err, ErrCacheNotStarted)))
}

func TestRedisCacheInvalidConnectionString(t *testing.T) {
	// A broken connection string is a configuration error and stays fatal.
	c := New(RedisCache, ConnectionString("http://localhost:6379"))
	qt.Check(t, qt.IsNotNil(c.Start(context.TODO())))
}

func TestClientCacheOptions(t *testing.T) {
	opt := newCacheOptions(ClientCacheTTL(time.Minute))
	qt.Check(t, qt.Equals(opt.ClientCacheTTL, time.Minute))
	qt.Check(t, qt.IsTrue(opt.ClientCache))
	qt.Check(t, qt.Equals(opt.ClientCacheSize, 0))

	// Instance level ClientCacheTTL(0) overrides the cache level default.
	opt = newCacheOptions(ClientCacheTTL(time.Minute), ClientCacheTTL(0))
	qt.Check(t, qt.Equals(opt.ClientCacheTTL, time.Duration(0)))

	opt = newCacheOptions(ClientCache(false), ClientCacheSize(8<<20))
	qt.Check(t, qt.IsFalse(opt.ClientCache))
	qt.Check(t, qt.Equals(opt.ClientCacheSize, 8<<20))
}

func TestRedisCacheClientCache(t *testing.T) {
	cs := getRedisConnStr()
	if cs == "" {
		t.Skip("REDIS_CONNSTR is not set")
	}
	c := New(RedisCache, ConnectionString(cs))
	err := c.Start(context.TODO())
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()

	hits := 0
	instr := func(_ context.Context, op string, _ ...any) func(error) {
		if op == InstrumentationGetHit {
			hits++
		}

		return instrumenter.NullFinish
	}

	i, err := Create[string](c, "cctest", ClientCacheTTL(time.Minute), Instrumenter(instr))
	qt.Assert(t, qt.IsNil(err))

	ctx := context.TODO()

	err = i.Set(ctx, "key", "value")
	qt.Assert(t, qt.IsNil(err))

	val, err := i.Get(ctx, "key")
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, "value"))

	// Second read is served from the local client-side cache.
	val, err = i.Get(ctx, "key")
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, "value"))
	qt.Check(t, qt.IsTrue(hits >= 1))

	// External write invalidates the locally cached value.
	con, err := c.Connection()
	qt.Assert(t, qt.IsNil(err))
	err = con.Do(ctx, con.B().Set().Key("cctest:key").Value(`"value2"`).Build()).Error()
	qt.Assert(t, qt.IsNil(err))

	for range 40 {
		val, err = i.Get(ctx, "key")
		qt.Assert(t, qt.IsNil(err))

		if val == "value2" {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	qt.Check(t, qt.Equals(val, "value2"))
}

func TestRedisCacheClientCacheDisabledFallback(t *testing.T) {
	cs := getRedisConnStr()
	if cs == "" {
		t.Skip("REDIS_CONNSTR is not set")
	}
	c := New(RedisCache, ConnectionString(cs), ClientCache(false), ClientCacheTTL(time.Minute))
	err := c.Start(context.TODO())
	qt.Assert(t, qt.IsNil(err))
	defer c.Close()

	i, err := Create[string](c, "ccdisabled")
	qt.Assert(t, qt.IsNil(err))

	ctx := context.TODO()

	err = i.Set(ctx, "key", "value")
	qt.Assert(t, qt.IsNil(err))

	val, err := i.Get(ctx, "key")
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(val, "value"))
}
