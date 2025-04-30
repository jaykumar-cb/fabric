/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecouchbase

import (
	"github.com/VictoriaMetrics/fastcache"
	"google.golang.org/protobuf/proto"
)

var keySep = []byte{0x00}

// cache holds both the system and user cache
type cache struct {
	sysCache      *fastcache.Cache
	usrCache      *fastcache.Cache
	sysNamespaces []string
}

// newCache creates a Cache. The cache consists of both system state cache (for lscc, _lifecycle)
// and user state cache (for all user deployed chaincodes). The size of the
// system state cache is 64 MB, by default. The size of the user state cache, in terms of MB, is
// specified via usrCacheSize parameter. Note that the maximum memory consumption of fastcache
// would be in the multiples of 32 MB (due to 512 buckets & an equal number of 64 KB chunks per bucket).
// If the usrCacheSizeMBs is not a multiple of 32 MB, the fastcache would round the size
// to the next multiple of 32 MB.
func newCache(usrCacheSizeMBs int, sysNamespaces []string) *cache {
	couchbaseLogger.Infof("Entering newCache() with usrCacheSizeMBs=%d, sysNamespaces=%v", usrCacheSizeMBs, sysNamespaces)

	cache := &cache{}
	// By default, 64 MB is allocated for the system cache
	cache.sysCache = fastcache.New(64 * 1024 * 1024)
	cache.sysNamespaces = sysNamespaces

	// User passed size is used to allocate memory for the user cache
	if usrCacheSizeMBs <= 0 {
		couchbaseLogger.Infof("User cache disabled (usrCacheSizeMBs=%d)", usrCacheSizeMBs)
		couchbaseLogger.Infof("Exiting newCache() with system cache only")
		return cache
	}
	cache.usrCache = fastcache.New(usrCacheSizeMBs * 1024 * 1024)
	couchbaseLogger.Infof("Exiting newCache() with system and user cache")
	return cache
}

// enabled returns true if the cache is enabled for a given namespace.
// Namespace can be of two types: system namespace (such as lscc) and user
// namespace (all user's chaincode states).
func (c *cache) enabled(namespace string) bool {
	couchbaseLogger.Infof("Entering cache.enabled() for namespace=%s", namespace)

	for _, ns := range c.sysNamespaces {
		if namespace == ns {
			couchbaseLogger.Infof("Exiting cache.enabled() for namespace=%s: true (system namespace)", namespace)
			return true
		}
	}

	isEnabled := c.usrCache != nil
	couchbaseLogger.Infof("Exiting cache.enabled() for namespace=%s: %v (user namespace)", namespace, isEnabled)
	return isEnabled
}

// getState returns the value for a given namespace and key from
// a cache associated with the chainID.
func (c *cache) getState(chainID, namespace, key string) (*CacheValueCouchbase, error) {
	couchbaseLogger.Infof("Entering getState() for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)

	cache := c.getCache(namespace)
	if cache == nil {
		couchbaseLogger.Infof("Exiting getState() with nil cache for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		return nil, nil
	}

	cacheKey := constructCacheKey(chainID, namespace, key)

	valBytes, exist := cache.HasGet(nil, cacheKey)
	if !exist {
		couchbaseLogger.Infof("Exiting getState() with cache miss for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		return nil, nil
	}

	cacheValue := &CacheValueCouchbase{}
	if err := proto.Unmarshal(valBytes, cacheValue); err != nil {
		couchbaseLogger.Errorf("Error unmarshalling cache value for chainID=%s, namespace=%s, key=%s: %+v", chainID, namespace, key, err)
		return nil, err
	}

	couchbaseLogger.Infof("Exiting getState() with cache hit for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
	return cacheValue, nil
}

// PutState stores a given value in a cache associated with the chainID.
func (c *cache) putState(chainID, namespace, key string, cacheValue *CacheValueCouchbase) error {
	couchbaseLogger.Infof("Entering putState() for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)

	cache := c.getCache(namespace)
	if cache == nil {
		couchbaseLogger.Infof("Exiting putState() with nil cache for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		return nil
	}

	cacheKey := constructCacheKey(chainID, namespace, key)
	valBytes, err := proto.Marshal(cacheValue)
	if err != nil {
		couchbaseLogger.Errorf("Error marshalling cache value for chainID=%s, namespace=%s, key=%s: %+v", chainID, namespace, key, err)
		return err
	}

	if cache.Has(cacheKey) {
		couchbaseLogger.Infof("Deleting existing cache entry for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		cache.Del(cacheKey)
	}

	cache.Set(cacheKey, valBytes)
	couchbaseLogger.Infof("Exiting putState() after setting cache for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
	return nil
}

// CacheUpdates is a map from a namespace to a set of cache KV updates
type cacheUpdates map[string]cacheKVs

// CacheKVs is a map from a key to a cache value
type cacheKVs map[string]*CacheValueCouchbase

// Add adds the given cacheKVs to the CacheUpdates
func (u cacheUpdates) add(namespace string, ckvs cacheKVs) {
	couchbaseLogger.Infof("Entering cacheUpdates.add() for namespace=%s with %d updates", namespace, len(ckvs))

	nsu, ok := u[namespace]
	if !ok {
		nsu = cacheKVs{}
		u[namespace] = nsu
		couchbaseLogger.Infof("Created new cache KVs for namespace=%s", namespace)
	}

	for k, v := range ckvs {
		nsu[k] = v
	}

	couchbaseLogger.Infof("Exiting cacheUpdates.add() for namespace=%s, total entries: %d", namespace, len(nsu))
}

// UpdateStates updates only the existing entries in the cache associated with
// the chainID.
func (c *cache) UpdateStates(chainID string, updates cacheUpdates) error {
	couchbaseLogger.Infof("Entering UpdateStates() for chainID=%s with updates for %d namespaces, updates: %+v", chainID, len(updates), updates)

	for ns, kvs := range updates {
		cache := c.getCache(ns)
		if cache == nil {
			couchbaseLogger.Infof("Skipping updates for namespace=%s, cache is nil", ns)
			continue
		}

		updateCount := 0
		deleteCount := 0
		for key, newVal := range kvs {
			couchbaseLogger.Infof("Updating value for key=%s, newVal=%s", key, newVal)
			cacheKey := constructCacheKey(chainID, ns, key)
			couchbaseLogger.Infof("CacheKey for key=%s: %s", key, cacheKey)
			if newVal == nil {
				couchbaseLogger.Infof("Deleting value for key=%s, newVal=%s", key, newVal)
				cache.Del(cacheKey)
				deleteCount++
				continue
			}

			if cache.Has(cacheKey) {
				newValBytes, err := proto.Marshal(newVal)
				if err != nil {
					couchbaseLogger.Errorf("Error marshalling cache value for chainID=%s, namespace=%s, key=%s: %+v", chainID, ns, key, err)
					return err
				}
				couchbaseLogger.Infof("Deleting existing value for key=%s, newVal=%s", key, newVal)
				cache.Del(cacheKey)
				cache.Set(cacheKey, newValBytes)
				couchbaseLogger.Infof("Set new value for key=%s, newVal=%s", key, newVal)
				updateCount++
			}
		}

		couchbaseLogger.Infof("Updated %d and deleted %d entries for namespace=%s", updateCount, deleteCount, ns)
	}

	couchbaseLogger.Infof("Exiting UpdateStates() for chainID=%s", chainID)
	return nil
}

// Reset removes all the items from the cache.
func (c *cache) Reset() {
	couchbaseLogger.Infof("Entering Reset()")

	c.sysCache.Reset()
	couchbaseLogger.Infof("Reset system cache")

	if c.usrCache != nil {
		c.usrCache.Reset()
		couchbaseLogger.Infof("Reset user cache")
	}

	couchbaseLogger.Infof("Exiting Reset()")
}

func (c *cache) getCache(namespace string) *fastcache.Cache {
	couchbaseLogger.Infof("Entering getCache() for namespace=%s", namespace)

	for _, ns := range c.sysNamespaces {
		if namespace == ns {
			couchbaseLogger.Infof("Exiting getCache() with system cache for namespace=%s", namespace)
			return c.sysCache
		}
	}

	if c.usrCache == nil {
		couchbaseLogger.Infof("Exiting getCache() with nil (user cache not enabled) for namespace=%s", namespace)
	} else {
		couchbaseLogger.Infof("Exiting getCache() with user cache for namespace=%s", namespace)
	}

	return c.usrCache
}

func constructCacheKey(chainID, namespace, key string) []byte {
	couchbaseLogger.Infof("Constructing cache key for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)

	var cacheKey []byte
	cacheKey = append(cacheKey, []byte(chainID)...)
	cacheKey = append(cacheKey, keySep...)
	cacheKey = append(cacheKey, []byte(namespace)...)
	cacheKey = append(cacheKey, keySep...)
	return append(cacheKey, []byte(key)...)
}
