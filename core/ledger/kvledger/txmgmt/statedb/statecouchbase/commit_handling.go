/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecouchbase

import (
	"math"
	"sync"

	"github.com/couchbase/gocb/v2"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
)

type committer struct {
	db           *couchbaseDatabase
	batchUpdates []gocb.BulkOp
	namespace    string
	cacheKVs     cacheKVs
	cacheEnabled bool
}

func (c *committer) addToCacheUpdate(kv *keyValue) {
	couchbaseLogger.Infof("[%s] Entering addToCacheUpdate() for key=%s", c.namespace, kv.key)

	if !c.cacheEnabled {
		couchbaseLogger.Infof("[%s] Cache not enabled, skipping addToCacheUpdate() for key=%s", c.namespace, kv.key)
		return
	}

	if kv.Value == nil {
		// nil value denotes a delete operation
		c.cacheKVs[kv.key] = nil
		couchbaseLogger.Infof("[%s] Added delete operation to cache for key=%s", c.namespace, kv.key)
		return
	}

	c.cacheKVs[kv.key] = &CacheValueCouchbase{
		Version:  kv.Version.ToBytes(),
		Value:    kv.Value,
		Metadata: kv.Metadata,
	}
	couchbaseLogger.Infof("[%s] Exiting addToCacheUpdate() for key=%s", c.namespace, kv.key)
}

// buildCommitters builds committers per namespace. Each committer transforms the
// given batch in the form of underlying db and keep it in memory.
func (vdb *VersionedDB) buildCommitters(updates *statedb.UpdateBatch) ([]*committer, error) {
	couchbaseLogger.Infof("Entering buildCommitters()")
	namespaces := updates.GetUpdatedNamespaces()
	couchbaseLogger.Infof("Building committers for namespaces: %v", namespaces)

	// for each namespace, we build multiple committers (based on maxBatchSize per namespace)
	var wg sync.WaitGroup
	nsCommittersChan := make(chan []*committer, len(namespaces))
	defer close(nsCommittersChan)
	errsChan := make(chan error, len(namespaces))
	defer close(errsChan)

	// for each namespace, we build committers in parallel. This is because,
	// the committer building process requires fetching of missing revisions
	// that in turn, we want to do in parallel
	for _, ns := range namespaces {
		nsUpdates := updates.GetUpdates(ns)
		wg.Add(1)
		go func(ns string) {
			defer wg.Done()
			couchbaseLogger.Infof("Building committers for namespace: %s", ns)
			committers, err := vdb.buildCommittersForNs(ns, nsUpdates)
			if err != nil {
				couchbaseLogger.Errorf("Error building committers for namespace %s: %+v", ns, err)
				errsChan <- err
				return
			}
			nsCommittersChan <- committers
			couchbaseLogger.Infof("Successfully built committers for namespace: %s", ns)
		}(ns)
	}
	wg.Wait()

	// collect all committers
	var allCommitters []*committer
	select {
	case err := <-errsChan:
		couchbaseLogger.Errorf("Error during buildCommitters: %+v", err)
		return nil, errors.WithStack(err)
	default:
		for i := 0; i < len(namespaces); i++ {
			allCommitters = append(allCommitters, <-nsCommittersChan...)
		}
	}

	couchbaseLogger.Infof("Exiting buildCommitters() with %d committers", len(allCommitters))
	return allCommitters, nil
}

func (vdb *VersionedDB) buildCommittersForNs(ns string, nsUpdates map[string]*statedb.VersionedValue) ([]*committer, error) {
	couchbaseLogger.Infof("[%s] Entering buildCommittersForNs() with %d updates", ns, len(nsUpdates))

	db, err := vdb.getNamespaceDBHandle(ns)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error getting namespace DB handle: %+v", ns, err)
		return nil, err
	}
	// for each namespace, build mutiple committers based on the maxBatchSize
	//maxBatchSize := db.couchbaseInstance.maxBatchUpdateSize()
	maxBatchSize := 1000
	numCommitters := 1
	if maxBatchSize > 0 {
		numCommitters = int(math.Ceil(float64(len(nsUpdates)) / float64(maxBatchSize)))
	}
	couchbaseLogger.Infof("[%s] Creating %d committers with batch size %d", ns, numCommitters, maxBatchSize)
	committers := make([]*committer, numCommitters)

	cacheEnabled := vdb.cache.enabled(ns)

	for i := 0; i < numCommitters; i++ {
		committers[i] = &committer{
			db:           db,
			batchUpdates: make([]gocb.BulkOp, 0),
			namespace:    ns,
			cacheKVs:     make(cacheKVs),
			cacheEnabled: cacheEnabled,
		}
	}

	i := 0
	for key, vv := range nsUpdates {
		kv := &keyValue{key: key, VersionedValue: vv}
		couchbaseDoc, err := keyValToCouchbaseDoc(kv)
		if err != nil {
			couchbaseLogger.Errorf("[%s] Error converting keyVal to couchbase doc for key %s: %+v", ns, key, err)
			return nil, err
		}
		committers[i].batchUpdates = append(committers[i].batchUpdates, &gocb.UpsertOp{
			ID:    (*couchbaseDoc)[idField].(string),
			Value: couchbaseDoc,
		})
		committers[i].addToCacheUpdate(kv)
		if maxBatchSize > 0 && len(committers[i].batchUpdates) == maxBatchSize {
			i++
		}
	}
	couchbaseLogger.Infof("[%s] Exiting buildCommittersForNs() with %d committers", ns, len(committers))
	return committers, nil
}

func (vdb *VersionedDB) executeCommitter(committers []*committer) error {
	couchbaseLogger.Infof("Entering executeCommitter() with %d committers", len(committers))

	errsChan := make(chan error, len(committers))
	defer close(errsChan)
	var wg sync.WaitGroup
	wg.Add(len(committers))

	for _, c := range committers {
		go func(c *committer) {
			defer wg.Done()
			couchbaseLogger.Infof("[%s] Executing committer with %d updates", c.namespace, len(c.batchUpdates))
			if err := c.commitUpdates(); err != nil {
				couchbaseLogger.Errorf("[%s] Error committing updates: %+v", c.namespace, err)
				errsChan <- err
			}
			couchbaseLogger.Infof("[%s] Successfully executed committer", c.namespace)
		}(c)
	}
	wg.Wait()

	select {
	case err := <-errsChan:
		couchbaseLogger.Errorf("Error during executeCommitter: %+v", err)
		return errors.WithStack(err)
	default:
		couchbaseLogger.Infof("Exiting executeCommitter() successfully")
		return nil
	}
}

// commitUpdates commits the given updates to couchdb
func (c *committer) commitUpdates() error {
	couchbaseLogger.Infof("[%s] Entering commitUpdates() with %d updates", c.namespace, len(c.batchUpdates))

	// Do the bulk update into couchdb. Note that this will do retries if the entire bulk update fails or times out
	err := c.db.batchUpdateDocuments(c.batchUpdates)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error batch updating documents: %+v", c.namespace, err)
		return err
	}

	couchbaseLogger.Infof("[%s] Exiting commitUpdates() successfully", c.namespace)
	return nil
}

// batchableDocument defines a document for a batch
type batchableDocument struct {
	UpsertDocs *gocb.BulkOp
	Deleted    bool
}
