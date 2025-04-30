/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecouchdb

import (
	"github.com/VictoriaMetrics/fastcache"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
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
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering newCache() with usrCacheSizeMBs=%d, sysNamespaces=%v", usrCacheSizeMBs, sysNamespaces)

	cache := &cache{}
	// By default, 64 MB is allocated for the system cache
	cache.sysCache = fastcache.New(64 * 1024 * 1024)
	cache.sysNamespaces = sysNamespaces

	// User passed size is used to allocate memory for the user cache
	if usrCacheSizeMBs <= 0 {
		logger.Infof("User cache disabled (usrCacheSizeMBs=%d)", usrCacheSizeMBs)
		logger.Infof("Exiting newCache() with system cache only")
		return cache
	}
	cache.usrCache = fastcache.New(usrCacheSizeMBs * 1024 * 1024)
	logger.Infof("Exiting newCache() with system and user cache")
	return cache
}

// enabled returns true if the cache is enabled for a given namespace.
// Namespace can be of two types: system namespace (such as lscc) and user
// namespace (all user's chaincode states).
func (c *cache) enabled(namespace string) bool {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering cache.enabled() for namespace=%s", namespace)

	for _, ns := range c.sysNamespaces {
		if namespace == ns {
			logger.Infof("Exiting cache.enabled() for namespace=%s: true (system namespace)", namespace)
			return true
		}
	}

	isEnabled := c.usrCache != nil
	logger.Infof("Exiting cache.enabled() for namespace=%s: %v (user namespace)", namespace, isEnabled)
	return isEnabled
}

// getState returns the value for a given namespace and key from
// a cache associated with the chainID.
func (c *cache) getState(chainID, namespace, key string) (*CacheValue, error) {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering getState() for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)

	cache := c.getCache(namespace)
	if cache == nil {
		logger.Infof("Exiting getState() with nil cache for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		return nil, nil
	}

	cacheKey := constructCacheKey(chainID, namespace, key)

	valBytes, exist := cache.HasGet(nil, cacheKey)
	if !exist {
		logger.Infof("Exiting getState() with cache miss for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		return nil, nil
	}

	cacheValue := &CacheValue{}
	if err := proto.Unmarshal(valBytes, cacheValue); err != nil {
		logger.Errorf("Error unmarshalling cache value for chainID=%s, namespace=%s, key=%s: %+v", chainID, namespace, key, err)
		return nil, err
	}

	logger.Infof("Exiting getState() with cache hit for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
	return cacheValue, nil
}

// PutState stores a given value in a cache associated with the chainID.
func (c *cache) putState(chainID, namespace, key string, cacheValue *CacheValue) error {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering putState() for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)

	cache := c.getCache(namespace)
	if cache == nil {
		logger.Infof("Exiting putState() with nil cache for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		return nil
	}

	cacheKey := constructCacheKey(chainID, namespace, key)
	valBytes, err := proto.Marshal(cacheValue)
	if err != nil {
		logger.Errorf("Error marshalling cache value for chainID=%s, namespace=%s, key=%s: %+v", chainID, namespace, key, err)
		return err
	}

	if cache.Has(cacheKey) {
		logger.Infof("Deleting existing cache entry for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
		cache.Del(cacheKey)
	}

	cache.Set(cacheKey, valBytes)
	logger.Infof("Exiting putState() after setting cache for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)
	return nil
}

// CacheUpdates is a map from a namespace to a set of cache KV updates
type cacheUpdates map[string]cacheKVs

// CacheKVs is a map from a key to a cache value
type cacheKVs map[string]*CacheValue

// Add adds the given cacheKVs to the CacheUpdates
func (u cacheUpdates) add(namespace string, ckvs cacheKVs) {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering cacheUpdates.add() for namespace=%s with %d updates", namespace, len(ckvs))

	nsu, ok := u[namespace]
	if !ok {
		nsu = cacheKVs{}
		u[namespace] = nsu
		logger.Infof("Created new cache KVs for namespace=%s", namespace)
	}

	for k, v := range ckvs {
		nsu[k] = v
	}

	logger.Infof("Exiting cacheUpdates.add() for namespace=%s, total entries: %d", namespace, len(nsu))
}

// UpdateStates updates only the existing entries in the cache associated with
// the chainID.
func (c *cache) UpdateStates(chainID string, updates cacheUpdates) error {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering UpdateStates() for chainID=%s with updates for %d namespaces, updates: %+v", chainID, len(updates), updates)

	for ns, kvs := range updates {
		cache := c.getCache(ns)
		if cache == nil {
			logger.Infof("Skipping updates for namespace=%s, cache is nil", ns)
			continue
		}

		updateCount := 0
		deleteCount := 0
		for key, newVal := range kvs {
			logger.Infof("Updating value for key=%s, newVal=%v", key, newVal)
			cacheKey := constructCacheKey(chainID, ns, key)
			logger.Infof("CacheKey for key=%s: %s", key, cacheKey)
			if newVal == nil {
				logger.Infof("Deleting value for key=%s, newVal=%v", key, newVal)
				cache.Del(cacheKey)
				deleteCount++
				continue
			}
			if cache.Has(cacheKey) {
				newValBytes, err := proto.Marshal(newVal)
				if err != nil {
					logger.Errorf("Error marshalling cache value for chainID=%s, namespace=%s, key=%s: %+v", chainID, ns, key, err)
					return err
				}
				logger.Infof("Deleting existing value for key=%s, newVal=%v", key, newVal)
				cache.Del(cacheKey)
				cache.Set(cacheKey, newValBytes)
				logger.Infof("Set new value for key=%s, newVal=%v", key, newVal)
				updateCount++
			}
		}

		logger.Infof("Updated %d and deleted %d entries for namespace=%s", updateCount, deleteCount, ns)
	}

	logger.Infof("Exiting UpdateStates() for chainID=%s", chainID)
	return nil
}

// Reset removes all the items from the cache.
func (c *cache) Reset() {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering Reset()")

	c.sysCache.Reset()
	logger.Infof("Reset system cache")

	if c.usrCache != nil {
		c.usrCache.Reset()
		logger.Infof("Reset user cache")
	}

	logger.Infof("Exiting Reset()")
}

func (c *cache) getCache(namespace string) *fastcache.Cache {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Entering getCache() for namespace=%s", namespace)

	for _, ns := range c.sysNamespaces {
		if namespace == ns {
			logger.Infof("Exiting getCache() with system cache for namespace=%s", namespace)
			return c.sysCache
		}
	}

	if c.usrCache == nil {
		logger.Infof("Exiting getCache() with nil (user cache not enabled) for namespace=%s", namespace)
	} else {
		logger.Infof("Exiting getCache() with user cache for namespace=%s", namespace)
	}

	return c.usrCache
}

func constructCacheKey(chainID, namespace, key string) []byte {
	logger := flogging.MustGetLogger("couchdb")
	logger.Infof("Constructing cache key for chainID=%s, namespace=%s, key=%s", chainID, namespace, key)

	var cacheKey []byte
	cacheKey = append(cacheKey, []byte(chainID)...)
	cacheKey = append(cacheKey, keySep...)
	cacheKey = append(cacheKey, []byte(namespace)...)
	cacheKey = append(cacheKey, keySep...)
	return append(cacheKey, []byte(key)...)
}
