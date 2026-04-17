package metrics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
)

// Compiler version strings — bumped whenever compilation semantics change so
// older cached artifacts cannot leak across deploys.
const (
	parserVersion   = "v1"
	semanticVersion = "v1"
	emitterVersion  = "v1"
	dialect         = "duckdb"
)

// ResolvedMetric is the immutable slice of MetricDef fields relevant to SQL
// compilation. Free-form fields like CreatedAt/UpdatedAt are intentionally
// excluded so cache keys stay stable across timestamp changes.
type ResolvedMetric struct {
	ID         string
	Kind       domain.MetricKind
	Expression string
	Frequency  domain.Frequency
	Version    int
	DependsOn  []string
}

// compileCache keeps a bounded entry count per SQL tier with TTL. It is the
// third tier (AST/IR/SQL) in the design doc; for phase 1 we collapse all three
// into one keyed map that encodes the full compile inputs, because today we
// have no separate IR evaluator. Upgrades can split the tiers without changing
// the caller contract.
type compileCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	maxSize int
	group   singleflight.Group
}

type cacheEntry struct {
	plan     *LogicalPlan
	expireAt time.Time
}

const (
	defaultCacheTTL = 5 * time.Minute
	defaultCacheMax = 512
)

func newCompileCache() *compileCache {
	return &compileCache{
		entries: make(map[string]*cacheEntry, defaultCacheMax),
		ttl:     defaultCacheTTL,
		maxSize: defaultCacheMax,
	}
}

func (c *compileCache) get(key string) (*LogicalPlan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ent, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(ent.expireAt) {
		delete(c.entries, key)
		return nil, false
	}
	return ent.plan, true
}

func (c *compileCache) put(key string, plan *LogicalPlan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxSize {
		// Simplest eviction: drop the earliest-expiring entry we can find.
		var oldestKey string
		var oldestTime time.Time
		for k, v := range c.entries {
			if oldestKey == "" || v.expireAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.expireAt
			}
		}
		if oldestKey != "" {
			delete(c.entries, oldestKey)
		}
	}
	c.entries[key] = &cacheEntry{plan: plan, expireAt: time.Now().Add(c.ttl)}
}

// CachedSQLCompiler wraps SQLCompiler with an in-memory LRU+TTL cache keyed by
// the inputs that actually affect the generated SQL (metric spec + dep graph +
// table existence). StartDate/EndDate are *not* part of the key — they only
// reach the cached plan's Args slice.
type CachedSQLCompiler struct {
	inner *SQLCompiler
	cache *compileCache
	db    datastore.QuantDB
}

// NewCachedSQLCompiler returns a compiler with compile caching enabled.
func NewCachedSQLCompiler(db datastore.QuantDB, parser domain.ExpressionParser, metricRepo domain.MetricDefRepository) *SQLCompiler {
	inner := NewSQLCompiler(db, parser, metricRepo)
	inner.cache = newCompileCache()
	return inner
}

// cachedCompile is the SQLCompiler entry point that consults the cache. The
// key is built from the stable metric spec, dep graph metadata, and table
// existence probes so cache hits are safe across date ranges / version bumps.
func (c *SQLCompiler) cachedCompile(ctx context.Context, metric *domain.MetricDef, startDate, endDate string) (*LogicalPlan, error) {
	if c.cache == nil {
		return c.compileFresh(ctx, metric, startDate, endDate)
	}
	key, err := c.cacheKey(ctx, metric)
	if err != nil {
		return c.compileFresh(ctx, metric, startDate, endDate)
	}
	if plan, ok := c.cache.get(key); ok {
		return rebindArgs(plan, startDate, endDate), nil
	}
	result, err, _ := c.cache.group.Do(key, func() (any, error) {
		if plan, ok := c.cache.get(key); ok {
			return plan, nil
		}
		plan, err := c.compileFresh(ctx, metric, startDate, endDate)
		if err != nil {
			return nil, err
		}
		c.cache.put(key, plan)
		return plan, nil
	})
	if err != nil {
		return nil, err
	}
	return rebindArgs(result.(*LogicalPlan), startDate, endDate), nil
}

// rebindArgs returns a copy of plan with its first two args (base CTE's
// startDate/endDate placeholders) swapped to the requested dates.
func rebindArgs(plan *LogicalPlan, startDate, endDate string) *LogicalPlan {
	if plan == nil {
		return nil
	}
	cp := *plan
	if len(plan.Args) >= 2 {
		cp.Args = append([]any(nil), plan.Args...)
		cp.Args[0] = startDate
		cp.Args[1] = endDate
	}
	if plan.Residual != nil {
		residualCopy := *plan.Residual
		if len(plan.Residual.InputArgs) >= 2 {
			residualCopy.InputArgs = append([]any(nil), plan.Residual.InputArgs...)
			residualCopy.InputArgs[0] = startDate
			residualCopy.InputArgs[1] = endDate
		}
		cp.Residual = &residualCopy
	}
	return &cp
}

// cacheKey hashes the metric + dep graph + table existence + compiler version
// stack into a single cache key.
func (c *SQLCompiler) cacheKey(ctx context.Context, metric *domain.MetricDef) (string, error) {
	dailyBasicExists, _ := c.db.TableExists(ctx, "daily_basic")
	moneyflowExists, _ := c.db.TableExists(ctx, "moneyflow")

	depKinds := make([]string, 0, len(metric.DependsOn))
	for _, depID := range metric.DependsOn {
		dep, err := c.metricRepo.Get(ctx, depID)
		if err != nil {
			return "", err
		}
		depKinds = append(depKinds, depID+":"+string(dep.Kind))
	}
	sort.Strings(depKinds)

	parts := []string{
		"parser=" + parserVersion,
		"semantic=" + semanticVersion,
		"emitter=" + emitterVersion,
		"dialect=" + dialect,
		"metric_id=" + metric.ID,
		"kind=" + string(metric.Kind),
		"frequency=" + string(metric.Frequency),
		"version=" + fmtInt(metric.Version),
		"expr=" + metric.Expression,
		"deps=" + strings.Join(depKinds, ","),
		"daily_basic=" + boolStr(dailyBasicExists),
		"moneyflow=" + boolStr(moneyflowExists),
	}
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:]), nil
}

func fmtInt(v int) string         { return fmt.Sprintf("%d", v) }
func boolStr(v bool) string       { if v { return "1" } ; return "0" }
