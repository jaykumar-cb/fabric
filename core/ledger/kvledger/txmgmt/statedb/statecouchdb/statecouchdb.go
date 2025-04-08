/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecouchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("statecouchdb")

const (
	// savepointDocID is used as a key for maintaining savepoint (maintained in metadatadb for a channel)
	savepointDocID = "statedb_savepoint"
	// channelMetadataDocID is used as a key to store the channel metadata for a channel (maintained in the channel's metadatadb).
	// Due to CouchDB's length restriction on db names, channel names and namepsaces may be truncated in db names.
	// The metadata is used for dropping channel-specific databases and snapshot support.
	channelMetadataDocID = "channel_metadata"
	// fabricInternalDBName is used to create a db in couch that would be used for internal data such as the version of the data format
	// a double underscore ensures that the dbname does not clash with the dbnames created for the chaincodes
	fabricInternalDBName = "fabric__internal"
	// dataformatVersionDocID is used as a key for maintaining version of the data format (maintained in fabric internal db)
	dataformatVersionDocID = "dataformatVersion"
)

var maxDataImportBatchMemorySize = 2 * 1024 * 1024

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
	couchInstance      *couchInstance
	databases          map[string]*VersionedDB
	mux                sync.Mutex
	openCounts         uint64
	redoLoggerProvider *redoLoggerProvider
	cache              *cache
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider(config *ledger.CouchDBConfig, metricsProvider metrics.Provider, sysNamespaces []string) (*VersionedDBProvider, error) {
	logger.Infof("Entering NewVersionedDBProvider() with config redoPath: %s, cache size: %d", config.RedoLogPath, config.UserCacheSizeMBs)
	logger.Infof("constructing CouchDB VersionedDBProvider")
	couchInstance, err := createCouchInstance(config, metricsProvider)
	if err != nil {
		logger.Infof("Exiting NewVersionedDBProvider() with error: %s", err)
		return nil, err
	}
	if err := checkExpectedDataformatVersion(couchInstance); err != nil {
		logger.Infof("Exiting NewVersionedDBProvider() with error: %s", err)
		return nil, err
	}
	p, err := newRedoLoggerProvider(config.RedoLogPath)
	if err != nil {
		logger.Infof("Exiting NewVersionedDBProvider() with error: %s", err)
		return nil, err
	}

	cache := newCache(config.UserCacheSizeMBs, sysNamespaces)
	provider := &VersionedDBProvider{
		couchInstance:      couchInstance,
		databases:          make(map[string]*VersionedDB),
		mux:                sync.Mutex{},
		openCounts:         0,
		redoLoggerProvider: p,
		cache:              cache,
	}
	logger.Infof("Exiting NewVersionedDBProvider() successfully")
	return provider, nil
}

func checkExpectedDataformatVersion(couchInstance *couchInstance) error {
	logger.Infof("Entering checkExpectedDataformatVersion()")
	databasesToIgnore := []string{fabricInternalDBName}
	isEmpty, err := couchInstance.isEmpty(databasesToIgnore)
	if err != nil {
		logger.Infof("Exiting checkExpectedDataformatVersion() with error: %s", err)
		return err
	}
	if isEmpty {
		logger.Infof("couch instance is empty. Setting dataformat version to %s", dataformat.CurrentFormat)
		err = writeDataFormatVersion(couchInstance, dataformat.CurrentFormat)
		if err != nil {
			logger.Infof("Exiting checkExpectedDataformatVersion() with error: %s", err)
			return err
		}
		logger.Infof("Exiting checkExpectedDataformatVersion() successfully")
		return nil
	}
	dataformatVersion, err := readDataformatVersion(couchInstance)
	if err != nil {
		logger.Infof("Exiting checkExpectedDataformatVersion() with error: %s", err)
		return err
	}
	if dataformatVersion != dataformat.CurrentFormat {
		formatErr := &dataformat.ErrFormatMismatch{
			DBInfo:         "CouchDB for state database",
			ExpectedFormat: dataformat.CurrentFormat,
			Format:         dataformatVersion,
		}
		logger.Infof("Exiting checkExpectedDataformatVersion() with error: %s", formatErr)
		return formatErr
	}
	logger.Infof("Exiting checkExpectedDataformatVersion() successfully")
	return nil
}

func readDataformatVersion(couchInstance *couchInstance) (string, error) {
	logger.Infof("Entering readDataformatVersion()")
	db, err := createCouchDatabase(couchInstance, fabricInternalDBName)
	if err != nil {
		logger.Infof("Exiting readDataformatVersion() with error: %s", err)
		return "", err
	}
	doc, _, err := db.readDoc(dataformatVersionDocID)
	if err != nil || doc == nil {
		logger.Infof("Exiting readDataformatVersion() with error: %s", err)
		return "", err
	}
	version, err := decodeDataformatInfo(doc)
	if err != nil {
		logger.Infof("Exiting readDataformatVersion() with error: %s", err)
		return "", err
	}
	logger.Infof("Exiting readDataformatVersion() with version: %s", version)
	return version, nil
}

func writeDataFormatVersion(couchInstance *couchInstance, dataformatVersion string) error {
	logger.Infof("Entering writeDataFormatVersion() with version: %s", dataformatVersion)
	db, err := createCouchDatabase(couchInstance, fabricInternalDBName)
	if err != nil {
		logger.Infof("Exiting writeDataFormatVersion() with error: %s", err)
		return err
	}
	doc, err := encodeDataformatInfo(dataformatVersion)
	if err != nil {
		logger.Infof("Exiting writeDataFormatVersion() with error: %s", err)
		return err
	}
	_, err = db.saveDoc(dataformatVersionDocID, "", doc)
	if err != nil {
		logger.Infof("Exiting writeDataFormatVersion() with error: %s", err)
		return err
	}
	logger.Infof("Exiting writeDataFormatVersion() successfully")
	return nil
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string, nsProvider statedb.NamespaceProvider) (statedb.VersionedDB, error) {
	logger.Infof("Entering GetDBHandle() with dbName: %s", dbName)
	provider.mux.Lock()
	defer provider.mux.Unlock()
	vdb := provider.databases[dbName]
	if vdb != nil {
		logger.Infof("Exiting GetDBHandle() with existing db handle")
		return vdb, nil
	}

	var err error
	vdb, err = newVersionedDB(
		provider.couchInstance,
		provider.redoLoggerProvider.newRedoLogger(dbName),
		dbName,
		provider.cache,
		nsProvider,
	)
	if err != nil {
		logger.Infof("Exiting GetDBHandle() with error: %s", err)
		return nil, err
	}
	provider.databases[dbName] = vdb
	logger.Infof("Exiting GetDBHandle() with new db handle")
	return vdb, nil
}

// ImportFromSnapshot loads the public state and pvtdata hashes from the snapshot files previously generated
func (provider *VersionedDBProvider) ImportFromSnapshot(
	dbName string,
	savepoint *version.Height,
	itr statedb.FullScanIterator,
) error {
	logger.Infof("Entering ImportFromSnapshot() with dbName: %s, savepoint height: %v", dbName, savepoint)
	metadataDB, err := createCouchDatabase(provider.couchInstance, constructMetadataDBName(dbName))
	if err != nil {
		errMsg := errors.WithMessagef(err, "error while creating the metadata database for channel %s", dbName)
		logger.Infof("Exiting ImportFromSnapshot() with error: %s", errMsg)
		return errMsg
	}

	vdb := &VersionedDB{
		chainName:     dbName,
		couchInstance: provider.couchInstance,
		metadataDB:    metadataDB,
		channelMetadata: &channelMetadata{
			ChannelName:      dbName,
			NamespaceDBsInfo: make(map[string]*namespaceDBInfo),
		},
		namespaceDBs: make(map[string]*couchDatabase),
	}
	if err := vdb.writeChannelMetadata(); err != nil {
		errMsg := errors.WithMessage(err, "error while writing channel metadata")
		logger.Infof("Exiting ImportFromSnapshot() with error: %s", errMsg)
		return errMsg
	}

	s := &snapshotImporter{
		vdb: vdb,
		itr: itr,
	}
	if err := s.importState(); err != nil {
		logger.Infof("Exiting ImportFromSnapshot() with error: %s", err)
		return err
	}

	err = vdb.recordSavepoint(savepoint)
	if err != nil {
		logger.Infof("Exiting ImportFromSnapshot() with error: %s", err)
		return err
	}

	logger.Infof("Exiting ImportFromSnapshot() successfully")
	return nil
}

// BytesKeySupported returns true if a db created supports bytes as a key
func (provider *VersionedDBProvider) BytesKeySupported() bool {
	logger.Infof("Entering BytesKeySupported()")
	logger.Infof("Exiting BytesKeySupported() with value: false")
	return false
}

// Close closes the underlying db instance
func (provider *VersionedDBProvider) Close() {
	logger.Infof("Entering Close()")
	// No close needed on Couch
	provider.redoLoggerProvider.close()
	logger.Infof("Exiting Close()")
}

// Drop drops the couch dbs and redologger data for the channel.
// It is not an error if a database does not exist.
func (provider *VersionedDBProvider) Drop(dbName string) error {
	logger.Infof("Entering Drop() with dbName: %s", dbName)
	metadataDBName := constructMetadataDBName(dbName)
	couchDBDatabase := couchDatabase{couchInstance: provider.couchInstance, dbName: metadataDBName}
	_, couchDBReturn, err := couchDBDatabase.getDatabaseInfo()
	if couchDBReturn != nil && couchDBReturn.StatusCode == http.StatusNotFound {
		// db does not exist
		logger.Infof("Exiting Drop() - DB does not exist")
		return nil
	}
	if err != nil {
		logger.Infof("Exiting Drop() with error: %s", err)
		return err
	}

	metadataDB, err := createCouchDatabase(provider.couchInstance, metadataDBName)
	if err != nil {
		logger.Infof("Exiting Drop() with error: %s", err)
		return err
	}
	channelMetadata, err := readChannelMetadata(metadataDB)
	if err != nil {
		logger.Infof("Exiting Drop() with error: %s", err)
		return err
	}

	for _, dbInfo := range channelMetadata.NamespaceDBsInfo {
		// do not drop metadataDB until all other dbs are dropped
		if dbInfo.DBName == metadataDBName {
			continue
		}
		if err := dropDB(provider.couchInstance, dbInfo.DBName); err != nil {
			logger.Errorw("Error dropping database", "channel", dbName, "namespace", dbInfo.Namespace, "error", err)
			logger.Infof("Exiting Drop() with error: %s", err)
			return err
		}
	}
	if err := dropDB(provider.couchInstance, metadataDBName); err != nil {
		logger.Errorw("Error dropping metadataDB", "channel", dbName, "error", err)
		logger.Infof("Exiting Drop() with error: %s", err)
		return err
	}

	delete(provider.databases, dbName)

	err = provider.redoLoggerProvider.leveldbProvider.Drop(dbName)
	if err != nil {
		logger.Infof("Exiting Drop() with error: %s", err)
		return err
	}

	logger.Infof("Exiting Drop() successfully")
	return nil
}

// HealthCheck checks to see if the couch instance of the peer is healthy
func (provider *VersionedDBProvider) HealthCheck(ctx context.Context) error {
	logger.Infof("Entering HealthCheck()")
	err := provider.couchInstance.healthCheck(ctx)
	if err != nil {
		logger.Infof("Exiting HealthCheck() with error: %s", err)
		return err
	}
	logger.Infof("Exiting HealthCheck() successfully")
	return nil
}

// VersionedDB implements VersionedDB interface
type VersionedDB struct {
	couchInstance      *couchInstance
	metadataDB         *couchDatabase            // A database per channel to store metadata such as savepoint.
	chainName          string                    // The name of the chain/channel.
	namespaceDBs       map[string]*couchDatabase // One database per namespace.
	channelMetadata    *channelMetadata          // Store channel name and namespaceDBInfo
	committedDataCache *versionsCache            // Used as a local cache during bulk processing of a block.
	verCacheLock       sync.RWMutex
	mux                sync.RWMutex
	redoLogger         *redoLogger
	cache              *cache
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(couchInstance *couchInstance, redoLogger *redoLogger, dbName string, cache *cache, nsProvider statedb.NamespaceProvider) (*VersionedDB, error) {
	logger.Infof("Entering newVersionedDB() with dbName: %s", dbName)
	// CreateCouchDatabase creates a CouchDB database object, as well as the underlying database if it does not exist
	chainName := dbName
	dbName = constructMetadataDBName(dbName)

	metadataDB, err := createCouchDatabase(couchInstance, dbName)
	if err != nil {
		logger.Infof("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}
	namespaceDBMap := make(map[string]*couchDatabase)
	vdb := &VersionedDB{
		couchInstance:      couchInstance,
		metadataDB:         metadataDB,
		chainName:          chainName,
		namespaceDBs:       namespaceDBMap,
		committedDataCache: newVersionCache(),
		redoLogger:         redoLogger,
		cache:              cache,
	}

	logger.Infof("chain [%s]: checking for redolog record", chainName)
	redologRecord, err := redoLogger.load()
	if err != nil {
		logger.Infof("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}
	savepoint, err := vdb.GetLatestSavePoint()
	if err != nil {
		logger.Infof("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}

	isNewDB := savepoint == nil
	if err = vdb.initChannelMetadata(isNewDB, nsProvider); err != nil {
		logger.Infof("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}

	// in normal circumstances, redolog is expected to be either equal to the last block
	// committed to the statedb or one ahead (in the event of a crash). However, either of
	// these or both could be nil on first time start (fresh start/rebuild)
	if redologRecord == nil || savepoint == nil {
		logger.Infof("chain [%s]: No redo-record or save point present", chainName)
		logger.Infof("Exiting newVersionedDB() successfully")
		return vdb, nil
	}

	logger.Infof("chain [%s]: save point = %#v, version of redolog record = %#v",
		chainName, savepoint, redologRecord.Version)

	if redologRecord.Version.BlockNum-savepoint.BlockNum == 1 {
		logger.Infof("chain [%s]: Re-applying last batch", chainName)
		if err := vdb.applyUpdates(redologRecord.UpdateBatch, redologRecord.Version); err != nil {
			logger.Infof("Exiting newVersionedDB() with error: %s", err)
			return nil, err
		}
	}
	logger.Infof("Exiting newVersionedDB() successfully")
	return vdb, nil
}

// getNamespaceDBHandle gets the handle to a named chaincode database
func (vdb *VersionedDB) getNamespaceDBHandle(namespace string) (*couchDatabase, error) {
	logger.Infof("Entering getNamespaceDBHandle() with namespace: %s", namespace)
	vdb.mux.RLock()
	db := vdb.namespaceDBs[namespace]
	vdb.mux.RUnlock()
	if db != nil {
		logger.Infof("Exiting getNamespaceDBHandle() with existing db handle")
		return db, nil
	}
	namespaceDBName := constructNamespaceDBName(vdb.chainName, namespace)
	vdb.mux.Lock()
	defer vdb.mux.Unlock()

	db = vdb.namespaceDBs[namespace]
	if db != nil {
		logger.Infof("Exiting getNamespaceDBHandle() with existing db handle")
		return db, nil
	}

	var err error
	if _, ok := vdb.channelMetadata.NamespaceDBsInfo[namespace]; !ok {
		logger.Infof("[%s] add namespaceDBInfo for namespace %s", vdb.chainName, namespace)
		vdb.channelMetadata.NamespaceDBsInfo[namespace] = &namespaceDBInfo{
			Namespace: namespace,
			DBName:    namespaceDBName,
		}
		if err = vdb.writeChannelMetadata(); err != nil {
			logger.Infof("Exiting getNamespaceDBHandle() with error: %s", err)
			return nil, err
		}
	}
	db, err = createCouchDatabase(vdb.couchInstance, namespaceDBName)
	if err != nil {
		logger.Infof("Exiting getNamespaceDBHandle() with error: %s", err)
		return nil, err
	}
	vdb.namespaceDBs[namespace] = db
	logger.Infof("Exiting getNamespaceDBHandle() with new db handle")
	return db, nil
}

// ProcessIndexesForChaincodeDeploy creates indexes for a specified namespace
func (vdb *VersionedDB) ProcessIndexesForChaincodeDeploy(namespace string, indexFilesData map[string][]byte) error {
	logger.Infof("Entering ProcessIndexesForChaincodeDeploy() with namespace: %s, %d index files", namespace, len(indexFilesData))
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting ProcessIndexesForChaincodeDeploy() with error: %s", err)
		return err
	}
	// We need to satisfy two requirements while processing the index files.
	// R1: all valid indexes should be processed.
	// R2: the order of index creation must be the same in all peers. For example, if user
	// passes two index files with the same index name but different index fields and we
	// process these files in different orders in different peers, each peer would
	// have different indexes (as one index definion could replace another if the index names
	// are the same).
	// To satisfy R1, we log the error and continue to process the next index file.
	// To satisfy R2, we sort the indexFilesData map based on the filenames and process
	// each index as per the sorted order.
	var indexFilesName []string
	for fileName := range indexFilesData {
		indexFilesName = append(indexFilesName, fileName)
	}
	sort.Strings(indexFilesName)
	for _, fileName := range indexFilesName {
		_, err = db.createIndex(string(indexFilesData[fileName]))
		switch {
		case err != nil:
			logger.Errorf("error creating index from file [%s] for chaincode [%s] on channel [%s]: %+v",
				fileName, namespace, vdb.chainName, err)
		default:
			logger.Infof("successfully submitted index creation request present in the file [%s] for chaincode [%s] on channel [%s]",
				fileName, namespace, vdb.chainName)
		}
	}
	logger.Infof("Exiting ProcessIndexesForChaincodeDeploy() successfully")
	return nil
}

// GetDBType returns the hosted stateDB
func (vdb *VersionedDB) GetDBType() string {
	logger.Infof("Entering GetDBType()")
	logger.Infof("Exiting GetDBType() with value: couchdb")
	return "couchdb"
}

// LoadCommittedVersions populates committedVersions and revisionNumbers into cache.
// A bulk retrieve from couchdb is used to populate the cache.
// committedVersions cache will be used for state validation of readsets
// revisionNumbers cache will be used during commit phase for couchdb bulk updates
func (vdb *VersionedDB) LoadCommittedVersions(keys []*statedb.CompositeKey) error {
	logger.Infof("Entering LoadCommittedVersions() with %d keys", len(keys))
	missingKeys := map[string][]string{}
	committedDataCache := newVersionCache()
	for _, compositeKey := range keys {
		ns, key := compositeKey.Namespace, compositeKey.Key
		committedDataCache.setVerAndRev(ns, key, nil, "")
		logger.Infof("Load into version cache: %s~%s", ns, key)

		if !vdb.cache.enabled(ns) {
			missingKeys[ns] = append(missingKeys[ns], key)
			continue
		}
		cv, err := vdb.cache.getState(vdb.chainName, ns, key)
		if err != nil {
			logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
			return err
		}
		if cv == nil {
			missingKeys[ns] = append(missingKeys[ns], key)
			continue
		}
		vv, err := constructVersionedValue(cv)
		if err != nil {
			logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
			return err
		}
		rev := string(cv.AdditionalInfo)
		committedDataCache.setVerAndRev(ns, key, vv.Version, rev)
	}

	nsMetadataMap, err := vdb.retrieveMetadata(missingKeys)
	logger.Infof("missingKeys=%s", missingKeys)
	logger.Infof("nsMetadataMap=%v", nsMetadataMap)
	if err != nil {
		logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
		return err
	}
	for ns, nsMetadata := range nsMetadataMap {
		for _, keyMetadata := range nsMetadata {
			// TODO - why would version be ever zero if loaded from db?
			if len(keyMetadata.Version) != 0 {
				version, _, err := decodeVersionAndMetadata(keyMetadata.Version)
				if err != nil {
					logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
					return err
				}
				committedDataCache.setVerAndRev(ns, keyMetadata.ID, version, keyMetadata.Rev)
			}
		}
	}
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = committedDataCache
	logger.Infof("Exiting LoadCommittedVersions() successfully")
	return nil
}

// GetVersion implements method in VersionedDB interface
func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	logger.Infof("Entering GetVersion() with namespace: %s, key: %s", namespace, key)
	version, keyFound := vdb.GetCachedVersion(namespace, key)
	if keyFound {
		logger.Infof("Exiting GetVersion() with cached version: %v", version)
		return version, nil
	}

	// This if block get executed only during simulation because during commit
	// we always call `LoadCommittedVersions` before calling `GetVersion`
	vv, err := vdb.GetState(namespace, key)
	if err != nil {
		logger.Infof("Exiting GetVersion() with error: %s", err)
		return nil, err
	}
	if vv == nil {
		logger.Infof("Exiting GetVersion() with nil version")
		return nil, nil
	}
	version = vv.Version
	logger.Infof("Exiting GetVersion() with version: %v", version)
	return version, nil
}

// GetCachedVersion returns version from cache. `LoadCommittedVersions` function populates the cache
func (vdb *VersionedDB) GetCachedVersion(namespace string, key string) (*version.Height, bool) {
	logger.Infof("Entering GetCachedVersion() with namespace: %s, key: %s", namespace, key)
	logger.Infof("Retrieving cached version: %s~%s", namespace, key)
	vdb.verCacheLock.RLock()
	defer vdb.verCacheLock.RUnlock()
	version, exists := vdb.committedDataCache.getVersion(namespace, key)
	logger.Infof("Exiting GetCachedVersion() with version: %v, exists: %t", version, exists)
	return version, exists
}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	logger.Infof("Entering ValidateKeyValue() with key: %s, value length: %d", key, len(value))
	err := validateKey(key)
	if err != nil {
		logger.Infof("Exiting ValidateKeyValue() with key validation error: %s", err)
		return err
	}
	err = validateValue(value)
	if err != nil {
		logger.Infof("Exiting ValidateKeyValue() with value validation error: %s", err)
		return err
	}
	logger.Infof("Exiting ValidateKeyValue() successfully")
	return nil
}

// BytesKeySupported implements method in VersionvdbedDB interface
func (vdb *VersionedDB) BytesKeySupported() bool {
	logger.Infof("Entering BytesKeySupported()")
	logger.Infof("Exiting BytesKeySupported() with value: false")
	return false
}

// GetState implements method in VersionedDB interface
func (vdb *VersionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	logger.Infof("Entering GetState() with namespace: %s, key: %s", namespace, key)
	logger.Infof("GetState(). ns=%s, key=%s", namespace, key)

	// (1) read the KV from the cache if available
	cacheEnabled := vdb.cache.enabled(namespace)
	if cacheEnabled {
		cv, err := vdb.cache.getState(vdb.chainName, namespace, key)
		if err != nil {
			logger.Infof("Exiting GetState() with error from cache: %s", err)
			return nil, err
		}
		if cv != nil {
			vv, err := constructVersionedValue(cv)
			if err != nil {
				logger.Infof("Exiting GetState() with error constructing value: %s", err)
				return nil, err
			}
			logger.Infof("Exiting GetState() with cached value, version: %v", vv.Version)
			return vv, nil
		}
	}

	// (2) read from the database if cache miss occurs
	kv, err := vdb.readFromDB(namespace, key)
	if err != nil {
		logger.Infof("Exiting GetState() with error from DB: %s", err)
		return nil, err
	}
	if kv == nil {
		logger.Infof("Exiting GetState() with nil value from DB")
		return nil, nil
	}

	// (3) if the value is not nil, store in the cache
	if cacheEnabled {
		cacheValue := constructCacheValue(kv.VersionedValue, kv.revision)
		if err := vdb.cache.putState(vdb.chainName, namespace, key, cacheValue); err != nil {
			logger.Infof("Exiting GetState() with error storing in cache: %s", err)
			return nil, err
		}
	}

	logger.Infof("Exiting GetState() with value from DB, version: %v", kv.VersionedValue.Version)
	return kv.VersionedValue, nil
}

func (vdb *VersionedDB) readFromDB(namespace, key string) (*keyValue, error) {
	logger.Infof("Entering readFromDB() with namespace: %s, key: %s", namespace, key)
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting readFromDB() with error getting DB handle: %s", err)
		return nil, err
	}
	if err := validateKey(key); err != nil {
		logger.Infof("Exiting readFromDB() with key validation error: %s", err)
		return nil, err
	}
	couchDoc, _, err := db.readDoc(key)
	if err != nil {
		logger.Infof("Exiting readFromDB() with error reading doc: %s", err)
		return nil, err
	}
	if couchDoc == nil {
		logger.Infof("Exiting readFromDB() with nil doc")
		return nil, nil
	}
	kv, err := couchDocToKeyValue(couchDoc)
	if err != nil {
		logger.Infof("Exiting readFromDB() with error converting to keyValue: %s", err)
		return nil, err
	}
	logger.Infof("Exiting readFromDB() with keyValue, version: %v", kv.VersionedValue.Version)
	return kv, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	logger.Infof("Entering GetStateMultipleKeys() with namespace: %s, keys: %v", namespace, keys)
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			logger.Infof("Exiting GetStateMultipleKeys() with error: %s", err)
			return nil, err
		}
		vals[i] = val
	}
	logger.Infof("Exiting GetStateMultipleKeys() with %d values", len(vals))
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	logger.Infof("Entering GetStateRangeScanIterator() with namespace: %s, startKey: %s, endKey: %s", namespace, startKey, endKey)
	iter, err := vdb.GetStateRangeScanIteratorWithPagination(namespace, startKey, endKey, 0)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIterator() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetStateRangeScanIterator() successfully")
	return iter, nil
}

// GetStateRangeScanIteratorWithPagination implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
// pageSize limits the number of results returned
func (vdb *VersionedDB) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (statedb.QueryResultsIterator, error) {
	logger.Infof("Entering GetStateRangeScanIteratorWithPagination() with namespace: %s, startKey: %s, endKey: %s, pageSize: %d", namespace, startKey, endKey, pageSize)
	logger.Infof("Entering GetStateRangeScanIteratorWithPagination namespace: %s  startKey: %s  endKey: %s  pageSize: %d", namespace, startKey, endKey, pageSize)
	internalQueryLimit := vdb.couchInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	scanner, err := newQueryScanner(namespace, db, "", internalQueryLimit, pageSize, "", startKey, endKey)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() successfully")
	return scanner, nil
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	logger.Infof("Entering ExecuteQuery() with namespace: %s, query: %s", namespace, query)
	queryResult, err := vdb.ExecuteQueryWithPagination(namespace, query, "", 0)
	if err != nil {
		logger.Infof("Exiting ExecuteQuery() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting ExecuteQuery() successfully")
	return queryResult, nil
}

// ExecuteQueryWithPagination implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (statedb.QueryResultsIterator, error) {
	logger.Infof("Entering ExecuteQueryWithPagination() with namespace: %s, query: %s, bookmark: %s, pageSize: %d", namespace, query, bookmark, pageSize)
	logger.Infof("Entering ExecuteQueryWithPagination namespace: %s,  query: %s,  bookmark: %s, pageSize: %d", namespace, query, bookmark, pageSize)
	internalQueryLimit := vdb.couchInstance.internalQueryLimit()
	queryString, err := applyAdditionalQueryOptions(query, internalQueryLimit, bookmark)
	if err != nil {
		logger.Errorf("Error calling applyAdditionalQueryOptions(): %s", err.Error())
		logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	scanner, err := newQueryScanner(namespace, db, queryString, internalQueryLimit, pageSize, bookmark, "", "")
	if err != nil {
		logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting ExecuteQueryWithPagination() successfully")
	return scanner, nil
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *VersionedDB) ApplyUpdates(updates *statedb.UpdateBatch, height *version.Height) error {
	logger.Infof("Entering ApplyUpdates() with height: %v, contains post order writes: %t", height, updates.ContainsPostOrderWrites)
	if height != nil && updates.ContainsPostOrderWrites {
		// height is passed nil when committing missing private data for previously committed blocks
		r := &redoRecord{
			UpdateBatch: updates,
			Version:     height,
		}
		if err := vdb.redoLogger.persist(r); err != nil {
			logger.Infof("Exiting ApplyUpdates() with error persisting to redoLogger: %s", err)
			return err
		}
	}
	err := vdb.applyUpdates(updates, height)
	if err != nil {
		logger.Infof("Exiting ApplyUpdates() with error: %s", err)
		return err
	}
	logger.Infof("Exiting ApplyUpdates() successfully")
	return nil
}

func (vdb *VersionedDB) applyUpdates(updates *statedb.UpdateBatch, height *version.Height) error {
	logger.Infof("Entering applyUpdates() with height: %v", height)
	// TODO a note about https://jira.hyperledger.org/browse/FAB-8622
	// The write lock is needed only for the stage 2.

	// stage 1 - buildCommitters builds committers per namespace (per DB). Each committer transforms the
	// given batch in the form of underlying db and keep it in memory.
	committers, err := vdb.buildCommitters(updates)
	if err != nil {
		logger.Infof("Exiting applyUpdates() with error building committers: %s", err)
		return err
	}

	// stage 2 -- executeCommitter executes each committer to push the changes to the DB
	if err = vdb.executeCommitter(committers); err != nil {
		logger.Infof("Exiting applyUpdates() with error executing committers: %s", err)
		return err
	}

	// Stgae 3 - postCommitProcessing - flush and record savepoint.
	namespaces := updates.GetUpdatedNamespaces()
	if err := vdb.postCommitProcessing(committers, namespaces, height); err != nil {
		logger.Infof("Exiting applyUpdates() with error in post commit processing: %s", err)
		return err
	}

	logger.Infof("Exiting applyUpdates() successfully")
	return nil
}

func (vdb *VersionedDB) postCommitProcessing(committers []*committer, namespaces []string, height *version.Height) error {
	logger.Infof("Entering postCommitProcessing() with %d committers, height: %v", len(committers), height)
	var wg sync.WaitGroup

	wg.Add(1)
	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		defer wg.Done()

		cacheUpdates := make(cacheUpdates)
		for _, c := range committers {
			if !c.cacheEnabled {
				continue
			}
			cacheUpdates.add(c.namespace, c.cacheKVs)
		}

		if len(cacheUpdates) == 0 {
			return
		}

		// update the cache
		if err := vdb.cache.UpdateStates(vdb.chainName, cacheUpdates); err != nil {
			vdb.cache.Reset()
			errChan <- err
		}
	}()

	// Record a savepoint at a given height
	if err := vdb.recordSavepoint(height); err != nil {
		logger.Errorf("Error during recordSavepoint: %s", err.Error())
		logger.Infof("Exiting postCommitProcessing() with recordSavepoint error: %s", err)
		return err
	}

	wg.Wait()
	select {
	case err := <-errChan:
		logger.Infof("Exiting postCommitProcessing() with error from cache update: %s", err)
		return errors.WithStack(err)
	default:
		logger.Infof("Exiting postCommitProcessing() successfully")
		return nil
	}
}

// ClearCachedVersions clears committedVersions and revisionNumbers
func (vdb *VersionedDB) ClearCachedVersions() {
	logger.Infof("Entering ClearCachedVersions()")
	logger.Infof("Clear Cache")
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = newVersionCache()
	logger.Infof("Exiting ClearCachedVersions()")
}

// Open implements method in VersionedDB interface
func (vdb *VersionedDB) Open() error {
	logger.Infof("Entering Open()")
	// no need to open db since a shared couch instance is used
	logger.Infof("Exiting Open() successfully")
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *VersionedDB) Close() {
	logger.Infof("Entering Close()")
	// no need to close db since a shared couch instance is used
	logger.Infof("Exiting Close()")
}

// recordSavepoint records a savepoint in the metadata db for the channel.
func (vdb *VersionedDB) recordSavepoint(height *version.Height) error {
	logger.Infof("Entering recordSavepoint() with height: %v", height)
	// If a given height is nil, it denotes that we are committing pvt data of old blocks.
	// In this case, we should not store a savepoint for recovery. The lastUpdatedOldBlockList
	// in the pvtstore acts as a savepoint for pvt data.
	if height == nil {
		logger.Infof("Exiting recordSavepoint() - nil height")
		return nil
	}
	savepointCouchDoc, err := encodeSavepoint(height)
	if err != nil {
		logger.Infof("Exiting recordSavepoint() with error encoding savepoint: %s", err)
		return err
	}
	_, err = vdb.metadataDB.saveDoc(savepointDocID, "", savepointCouchDoc)
	if err != nil {
		logger.Errorf("Failed to save the savepoint to DB %s", err.Error())
		logger.Infof("Exiting recordSavepoint() with error saving document: %s", err)
		return err
	}
	logger.Infof("Exiting recordSavepoint() successfully")
	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
	logger.Infof("Entering GetLatestSavePoint()")
	var err error
	couchDoc, _, err := vdb.metadataDB.readDoc(savepointDocID)
	if err != nil {
		logger.Errorf("Failed to read savepoint data %s", err.Error())
		logger.Infof("Exiting GetLatestSavePoint() with error: %s", err)
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return height nil
	if couchDoc == nil || couchDoc.jsonValue == nil {
		logger.Infof("Exiting GetLatestSavePoint() with nil savepoint")
		return nil, nil
	}
	height, err := decodeSavepoint(couchDoc)
	if err != nil {
		logger.Infof("Exiting GetLatestSavePoint() with error decoding savepoint: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetLatestSavePoint() with height: %v", height)
	return height, nil
}

// initChannelMetadata initizlizes channelMetadata and build NamespaceDBInfo mapping if not present
func (vdb *VersionedDB) initChannelMetadata(isNewDB bool, namespaceProvider statedb.NamespaceProvider) error {
	logger.Infof("Entering initChannelMetadata() with isNewDB: %t", isNewDB)
	// create channelMetadata with empty NamespaceDBInfo mapping for a new DB
	if isNewDB {
		vdb.channelMetadata = &channelMetadata{
			ChannelName:      vdb.chainName,
			NamespaceDBsInfo: make(map[string]*namespaceDBInfo),
		}
		err := vdb.writeChannelMetadata()
		if err != nil {
			logger.Infof("Exiting initChannelMetadata() with error writing metadata: %s", err)
			return err
		}
		logger.Infof("Exiting initChannelMetadata() successfully - created new metadata")
		return nil
	}

	// read stored channelMetadata from an existing DB
	var err error
	vdb.channelMetadata, err = vdb.readChannelMetadata()
	if err != nil {
		logger.Infof("Exiting initChannelMetadata() with error reading metadata: %s", err)
		return err
	}
	if vdb.channelMetadata != nil {
		logger.Infof("Exiting initChannelMetadata() successfully - read existing metadata")
		return nil
	}

	// channelMetadata is not present - this is the case when opening older dbs (e.g., v2.0/v2.1) for the first time
	// create channelMetadata and build NamespaceDBInfo mapping retroactively
	vdb.channelMetadata = &channelMetadata{
		ChannelName:      vdb.chainName,
		NamespaceDBsInfo: make(map[string]*namespaceDBInfo),
	}
	// retrieve existing DB names
	dbNames, err := vdb.couchInstance.retrieveApplicationDBNames()
	if err != nil {
		logger.Infof("Exiting initChannelMetadata() with error retrieving DB names: %s", err)
		return err
	}
	existingDBNames := make(map[string]struct{}, len(dbNames))
	for _, dbName := range dbNames {
		existingDBNames[dbName] = struct{}{}
	}
	// get namespaces and add a namespace to channelMetadata only if its DB name already exists
	namespaces, err := namespaceProvider.PossibleNamespaces(vdb)
	if err != nil {
		logger.Infof("Exiting initChannelMetadata() with error getting namespaces: %s", err)
		return err
	}
	for _, ns := range namespaces {
		dbName := constructNamespaceDBName(vdb.chainName, ns)
		if _, ok := existingDBNames[dbName]; ok {
			vdb.channelMetadata.NamespaceDBsInfo[ns] = &namespaceDBInfo{
				Namespace: ns,
				DBName:    dbName,
			}
		}
	}
	err = vdb.writeChannelMetadata()
	if err != nil {
		logger.Infof("Exiting initChannelMetadata() with error writing retroactive metadata: %s", err)
		return err
	}
	logger.Infof("Exiting initChannelMetadata() successfully - created retroactive metadata")
	return nil
}

// readChannelMetadata returns channel metadata stored in metadataDB
func (vdb *VersionedDB) readChannelMetadata() (*channelMetadata, error) {
	logger.Infof("Entering readChannelMetadata()")
	metadata, err := readChannelMetadata(vdb.metadataDB)
	if err != nil {
		logger.Infof("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting readChannelMetadata() with metadata: %v", metadata)
	return metadata, nil
}

func readChannelMetadata(metadataDB *couchDatabase) (*channelMetadata, error) {
	logger.Infof("Entering readChannelMetadata() with metadataDB")
	var err error
	couchDoc, _, err := metadataDB.readDoc(channelMetadataDocID)
	if err != nil {
		logger.Errorf("Failed to read db name mapping data %s", err.Error())
		logger.Infof("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return nil
	if couchDoc == nil || couchDoc.jsonValue == nil {
		logger.Infof("Exiting readChannelMetadata() with nil - no document found")
		return nil, nil
	}
	metadata, err := decodeChannelMetadata(couchDoc)
	if err != nil {
		logger.Infof("Exiting readChannelMetadata() with error decoding metadata: %s", err)
		return nil, err
	}
	logger.Infof("Exiting readChannelMetadata() with metadata: %v", metadata)
	return metadata, nil
}

// writeChannelMetadata saves channel metadata to metadataDB
func (vdb *VersionedDB) writeChannelMetadata() error {
	logger.Infof("Entering writeChannelMetadata()")
	couchDoc, err := encodeChannelMetadata(vdb.channelMetadata)
	if err != nil {
		logger.Infof("Exiting writeChannelMetadata() with error encoding metadata: %s", err)
		return err
	}
	_, err = vdb.metadataDB.saveDoc(channelMetadataDocID, "", couchDoc)
	if err != nil {
		logger.Infof("Exiting writeChannelMetadata() with error saving doc: %s", err)
		return err
	}
	logger.Infof("Exiting writeChannelMetadata() successfully")
	return nil
}

// GetFullScanIterator implements method in VersionedDB interface. This function returns a
// FullScanIterator that can be used to iterate over entire data in the statedb for a channel.
// `skipNamespace` parameter can be used to control if the consumer wants the FullScanIterator
// to skip one or more namespaces from the returned results.
func (vdb *VersionedDB) GetFullScanIterator(skipNamespace func(string) bool) (statedb.FullScanIterator, error) {
	logger.Infof("Entering GetFullScanIterator()")
	namespacesToScan := []string{}
	for ns := range vdb.channelMetadata.NamespaceDBsInfo {
		if skipNamespace(ns) {
			continue
		}
		namespacesToScan = append(namespacesToScan, ns)
	}
	sort.Strings(namespacesToScan)

	// if namespacesToScan is empty, we can return early with a nil FullScanIterator. However,
	// the implementation of this method needs be consistent with the same method implemented in
	// the stateleveldb pkg. Hence, we don't return a nil FullScanIterator by checking the length
	// of the namespacesToScan.

	dbsToScan := []*namespaceDB{}
	for _, ns := range namespacesToScan {
		db, err := vdb.getNamespaceDBHandle(ns)
		if err != nil {
			errMsg := errors.WithMessagef(err, "failed to get database handle for the namespace %s", ns)
			logger.Infof("Exiting GetFullScanIterator() with error: %s", errMsg)
			return nil, errMsg
		}
		dbsToScan = append(dbsToScan, &namespaceDB{ns, db})
	}

	// the database which belong to an empty namespace contains
	// internal keys. The scanner must skip these keys.
	toSkipKeysFromEmptyNs := map[string]bool{
		savepointDocID:       true,
		channelMetadataDocID: true,
	}
	scanner, err := newDBsScanner(dbsToScan, vdb.couchInstance.internalQueryLimit(), toSkipKeysFromEmptyNs)
	if err != nil {
		logger.Infof("Exiting GetFullScanIterator() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetFullScanIterator() with scanner for %d namespaces", len(namespacesToScan))
	return scanner, nil
}

// applyAdditionalQueryOptions will add additional fields to the query required for query processing
func applyAdditionalQueryOptions(queryString string, queryLimit int32, queryBookmark string) (string, error) {
	logger.Infof("Entering applyAdditionalQueryOptions() with queryString: %s, queryLimit: %d, queryBookmark: %s", queryString, queryLimit, queryBookmark)
	const jsonQueryFields = "fields"
	const jsonQueryLimit = "limit"
	const jsonQueryBookmark = "bookmark"
	// create a generic map for the query json
	jsonQueryMap := make(map[string]interface{})
	// unmarshal the selector json into the generic map
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(queryString)))
	decoder.UseNumber()
	err := decoder.Decode(&jsonQueryMap)
	if err != nil {
		logger.Infof("Exiting applyAdditionalQueryOptions() with error decoding: %s", err)
		return "", err
	}
	if fieldsJSONArray, ok := jsonQueryMap[jsonQueryFields]; ok {
		switch fieldsJSONArray := fieldsJSONArray.(type) {
		case []interface{}:
			// Add the "_id", and "version" fields,  these are needed by default
			jsonQueryMap[jsonQueryFields] = append(fieldsJSONArray, idField, versionField)
		default:
			err := errors.New("fields definition must be an array")
			logger.Infof("Exiting applyAdditionalQueryOptions() with error: %s", err)
			return "", err
		}
	}
	// Add limit
	// This will override any limit passed in the query.
	// Explicit paging not yet supported.
	jsonQueryMap[jsonQueryLimit] = queryLimit
	// Add the bookmark if provided
	if queryBookmark != "" {
		jsonQueryMap[jsonQueryBookmark] = queryBookmark
	}
	// Marshal the updated json query
	editedQuery, err := json.Marshal(jsonQueryMap)
	if err != nil {
		logger.Infof("Exiting applyAdditionalQueryOptions() with error marshaling: %s", err)
		return "", err
	}
	logger.Infof("Rewritten query: %s", editedQuery)
	logger.Infof("Exiting applyAdditionalQueryOptions() with updated query")
	return string(editedQuery), nil
}

type queryScanner struct {
	namespace       string
	db              *couchDatabase
	queryDefinition *queryDefinition
	paginationInfo  *paginationInfo
	resultsInfo     *resultsInfo
	exhausted       bool
}

type queryDefinition struct {
	startKey           string
	endKey             string
	query              string
	internalQueryLimit int32
}

type paginationInfo struct {
	cursor         int32
	requestedLimit int32
	bookmark       string
}

type resultsInfo struct {
	totalRecordsReturned int32
	results              []*queryResult
}

func newQueryScanner(namespace string, db *couchDatabase, query string, internalQueryLimit,
	limit int32, bookmark, startKey, endKey string) (*queryScanner, error) {
	scanner := &queryScanner{namespace, db, &queryDefinition{startKey, endKey, query, internalQueryLimit}, &paginationInfo{-1, limit, bookmark}, &resultsInfo{0, nil}, false}
	var err error
	// query is defined, then execute the query and return the records and bookmark
	if scanner.queryDefinition.query != "" {
		err = scanner.executeQueryWithBookmark()
	} else {
		err = scanner.getNextStateRangeScanResults()
	}
	if err != nil {
		return nil, err
	}
	scanner.paginationInfo.cursor = -1
	return scanner, nil
}

func rangeScanFilterCouchInternalDocs(db *couchDatabase,
	startKey, endKey string, queryLimit int32,
) ([]*queryResult, string, error) {
	var finalResults []*queryResult
	var finalNextStartKey string
	for {
		results, nextStartKey, err := db.readDocRange(startKey, endKey, queryLimit)
		if err != nil {
			logger.Infof("Error calling ReadDocRange(): %s\n", err.Error())
			return nil, "", err
		}
		var filteredResults []*queryResult
		for _, doc := range results {
			if !isCouchInternalKey(doc.id) {
				filteredResults = append(filteredResults, doc)
			}
		}

		finalResults = append(finalResults, filteredResults...)
		finalNextStartKey = nextStartKey
		queryLimit = int32(len(results) - len(filteredResults))
		if queryLimit == 0 || finalNextStartKey == "" {
			break
		}
		startKey = finalNextStartKey
	}
	var err error
	for i := 0; isCouchInternalKey(finalNextStartKey); i++ {
		_, finalNextStartKey, err = db.readDocRange(finalNextStartKey, endKey, 1)
		logger.Infof("i=%d, finalNextStartKey=%s", i, finalNextStartKey)
		if err != nil {
			return nil, "", err
		}
	}
	return finalResults, finalNextStartKey, nil
}

func isCouchInternalKey(key string) bool {
	return len(key) != 0 && key[0] == '_'
}

func (scanner *queryScanner) getNextStateRangeScanResults() error {
	logger.Infof("Entering getNextStateRangeScanResults() with startKey: %s, endKey: %s", scanner.queryDefinition.startKey, scanner.queryDefinition.endKey)
	queryLimit := scanner.queryDefinition.internalQueryLimit
	if scanner.paginationInfo.requestedLimit > 0 {
		moreResultsNeeded := scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
		if moreResultsNeeded < scanner.queryDefinition.internalQueryLimit {
			queryLimit = moreResultsNeeded
		}
	}
	queryResult, nextStartKey, err := rangeScanFilterCouchInternalDocs(scanner.db,
		scanner.queryDefinition.startKey, scanner.queryDefinition.endKey, queryLimit)
	if err != nil {
		logger.Infof("Exiting getNextStateRangeScanResults() with error: %s", err)
		return err
	}
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.cursor = 0
	if scanner.queryDefinition.endKey == nextStartKey {
		// as we always set inclusive_end=false to match the behavior of
		// goleveldb iterator, it is safe to mark the scanner as exhausted
		scanner.exhausted = true
		// we still need to update the startKey as it is returned as bookmark
	}
	scanner.queryDefinition.startKey = nextStartKey
	logger.Infof("Exiting getNextStateRangeScanResults() with %d results, nextStartKey: %s", len(queryResult), nextStartKey)
	return nil
}

// executeQueryWithBookmark executes a "paging" query with a bookmark, this method allows a
// paged query without returning a new query iterator
func (scanner *queryScanner) executeQueryWithBookmark() error {
	logger.Infof("Entering executeQueryWithBookmark() with query: %s, bookmark: %s", scanner.queryDefinition.query, scanner.paginationInfo.bookmark)
	queryLimit := scanner.queryDefinition.internalQueryLimit
	if scanner.paginationInfo.requestedLimit > 0 {
		if scanner.paginationInfo.requestedLimit-scanner.resultsInfo.totalRecordsReturned < scanner.queryDefinition.internalQueryLimit {
			queryLimit = scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
		}
	}
	queryString, err := applyAdditionalQueryOptions(scanner.queryDefinition.query,
		queryLimit, scanner.paginationInfo.bookmark)
	if err != nil {
		logger.Infof("Error calling applyAdditionalQueryOptions(): %s\n", err.Error())
		logger.Infof("Exiting executeQueryWithBookmark() with error: %s", err)
		return err
	}
	queryResult, bookmark, err := scanner.db.queryDocuments(queryString)
	if err != nil {
		logger.Infof("Error calling QueryDocuments(): %s\n", err.Error())
		logger.Infof("Exiting executeQueryWithBookmark() with error: %s", err)
		return err
	}
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.bookmark = bookmark
	scanner.paginationInfo.cursor = 0
	logger.Infof("Exiting executeQueryWithBookmark() with %d results", len(queryResult))
	return nil
}

func (scanner *queryScanner) Next() (*statedb.VersionedKV, error) {
	logger.Infof("Entering Next() for queryScanner")
	doc, err := scanner.next()
	if err != nil {
		logger.Infof("Exiting Next() with error: %s", err)
		return nil, err
	}
	if doc == nil {
		logger.Infof("Exiting Next() with nil result")
		return nil, nil
	}
	kv, err := couchDocToKeyValue(doc)
	if err != nil {
		logger.Infof("Exiting Next() with error: %s", err)
		return nil, err
	}
	scanner.resultsInfo.totalRecordsReturned++
	result := &statedb.VersionedKV{
		CompositeKey: &statedb.CompositeKey{
			Namespace: scanner.namespace,
			Key:       kv.key,
		},
		VersionedValue: kv.VersionedValue,
	}
	logger.Infof("Exiting Next() with key: %s", kv.key)
	return result, nil
}

func (scanner *queryScanner) next() (*couchDoc, error) {
	logger.Infof("Entering next() with cursor: %d, results len: %d", scanner.paginationInfo.cursor, len(scanner.resultsInfo.results))
	if len(scanner.resultsInfo.results) == 0 {
		logger.Infof("Exiting next() with nil result - no results available")
		return nil, nil
	}
	scanner.paginationInfo.cursor++
	if scanner.paginationInfo.cursor >= scanner.queryDefinition.internalQueryLimit {
		if scanner.exhausted {
			logger.Infof("Exiting next() with nil result - scanner exhausted")
			return nil, nil
		}
		var err error
		if scanner.queryDefinition.query != "" {
			err = scanner.executeQueryWithBookmark()
		} else {
			err = scanner.getNextStateRangeScanResults()
		}
		if err != nil {
			logger.Infof("Exiting next() with error: %s", err)
			return nil, err
		}
		if len(scanner.resultsInfo.results) == 0 {
			logger.Infof("Exiting next() with nil result - no more results")
			return nil, nil
		}
	}
	if scanner.paginationInfo.cursor >= int32(len(scanner.resultsInfo.results)) {
		logger.Infof("Exiting next() with nil result - cursor beyond results")
		return nil, nil
	}
	result := scanner.resultsInfo.results[scanner.paginationInfo.cursor]
	couchDoc := &couchDoc{
		jsonValue:   result.value,
		attachments: result.attachments,
	}
	logger.Infof("Exiting next() with document")
	return couchDoc, nil
}

func (scanner *queryScanner) Close() {
	logger.Infof("Entering Close() for queryScanner")
	logger.Infof("Exiting Close()")
}

func (scanner *queryScanner) GetBookmarkAndClose() string {
	logger.Infof("Entering GetBookmarkAndClose()")
	retval := ""
	if scanner.queryDefinition.query != "" {
		retval = scanner.paginationInfo.bookmark
	} else {
		retval = scanner.queryDefinition.startKey
	}
	scanner.Close()
	logger.Infof("Exiting GetBookmarkAndClose() with bookmark: %s", retval)
	return retval
}

func constructCacheValue(v *statedb.VersionedValue, rev string) *CacheValue {
	return &CacheValue{
		Version:        v.Version.ToBytes(),
		Value:          v.Value,
		Metadata:       v.Metadata,
		AdditionalInfo: []byte(rev),
	}
}

func constructVersionedValue(cv *CacheValue) (*statedb.VersionedValue, error) {
	height, _, err := version.NewHeightFromBytes(cv.Version)
	if err != nil {
		return nil, err
	}

	return &statedb.VersionedValue{
		Value:    cv.Value,
		Version:  height,
		Metadata: cv.Metadata,
	}, nil
}

type dbsScanner struct {
	dbs                   []*namespaceDB
	nextDBToScanIndex     int
	resultItr             *queryScanner
	currentNamespace      string
	prefetchLimit         int32
	toSkipKeysFromEmptyNs map[string]bool
}

type namespaceDB struct {
	ns string
	db *couchDatabase
}

func newDBsScanner(dbsToScan []*namespaceDB, prefetchLimit int32, toSkipKeysFromEmptyNs map[string]bool) (*dbsScanner, error) {
	if len(dbsToScan) == 0 {
		return nil, nil
	}
	s := &dbsScanner{
		dbs:                   dbsToScan,
		prefetchLimit:         prefetchLimit,
		toSkipKeysFromEmptyNs: toSkipKeysFromEmptyNs,
	}
	if err := s.beginNextDBScan(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *dbsScanner) beginNextDBScan() error {
	dbUnderScan := s.dbs[s.nextDBToScanIndex]
	queryScanner, err := newQueryScanner(dbUnderScan.ns, dbUnderScan.db, "", s.prefetchLimit, 0, "", "", "")
	if err != nil {
		return errors.WithMessagef(
			err,
			"failed to create a query scanner for the database %s associated with the namespace %s",
			dbUnderScan.db.dbName,
			dbUnderScan.ns,
		)
	}
	s.resultItr = queryScanner
	s.currentNamespace = dbUnderScan.ns
	s.nextDBToScanIndex++
	return nil
}

// Next returns the key-values present in the namespaceDB. Once a namespaceDB
// is processed, it moves to the next namespaceDB till all are processed.
func (s *dbsScanner) Next() (*statedb.VersionedKV, error) {
	if s == nil {
		return nil, nil
	}
	for {
		couchDoc, err := s.resultItr.next()
		if err != nil {
			return nil, errors.WithMessagef(
				err,
				"failed to retrieve the next entry from scanner associated with namespace %s",
				s.currentNamespace,
			)
		}
		if couchDoc == nil {
			s.resultItr.Close()
			if len(s.dbs) <= s.nextDBToScanIndex {
				break
			}
			if err := s.beginNextDBScan(); err != nil {
				return nil, err
			}
			continue
		}
		if s.currentNamespace == "" {
			key, err := couchDoc.key()
			if err != nil {
				return nil, errors.WithMessagef(
					err,
					"failed to retrieve key from the couchdoc present in the empty namespace",
				)
			}
			if s.toSkipKeysFromEmptyNs[key] {
				continue
			}
		}
		kv, err := couchDocToKeyValue(couchDoc)
		if err != nil {
			return nil, errors.WithMessagef(
				err,
				"failed to validate and retrieve fields from couch doc with id %s",
				kv.key,
			)
		}
		return &statedb.VersionedKV{
				CompositeKey: &statedb.CompositeKey{
					Namespace: s.currentNamespace,
					Key:       kv.key,
				},
				VersionedValue: kv.VersionedValue,
			},
			nil
	}
	return nil, nil
}

func (s *dbsScanner) Close() {
	if s == nil {
		return
	}
	s.resultItr.Close()
}

type snapshotImporter struct {
	vdb              *VersionedDB
	itr              statedb.FullScanIterator
	currentNs        string
	currentNsDB      *couchDatabase
	pendingDocsBatch []*couchDoc
	batchMemorySize  int
}

func (s *snapshotImporter) importState() error {
	logger.Infof("Entering importState()")
	if s.itr == nil {
		logger.Infof("Exiting importState() successfully - nil iterator")
		return nil
	}
	for {
		versionedKV, err := s.itr.Next()
		if err != nil {
			logger.Infof("Exiting importState() with error: %s", err)
			return err
		}
		if versionedKV == nil {
			break
		}

		switch {
		case s.currentNsDB == nil:
			if err := s.createDBForNamespace(versionedKV.Namespace); err != nil {
				logger.Infof("Exiting importState() with error: %s", err)
				return err
			}
		case s.currentNs != versionedKV.Namespace:
			if err := s.storePendingDocs(); err != nil {
				logger.Infof("Exiting importState() with error: %s", err)
				return err
			}
			if err := s.createDBForNamespace(versionedKV.Namespace); err != nil {
				logger.Infof("Exiting importState() with error: %s", err)
				return err
			}
		}

		doc, err := keyValToCouchDoc(
			&keyValue{
				key:            versionedKV.Key,
				VersionedValue: versionedKV.VersionedValue,
			},
		)
		if err != nil {
			logger.Infof("Exiting importState() with error: %s", err)
			return err
		}
		s.pendingDocsBatch = append(s.pendingDocsBatch, doc)
		s.batchMemorySize += doc.len()

		if s.batchMemorySize >= maxDataImportBatchMemorySize ||
			len(s.pendingDocsBatch) == s.vdb.couchInstance.maxBatchUpdateSize() {
			if err := s.storePendingDocs(); err != nil {
				logger.Infof("Exiting importState() with error: %s", err)
				return err
			}
		}
	}

	err := s.storePendingDocs()
	if err != nil {
		logger.Infof("Exiting importState() with error: %s", err)
		return err
	}
	logger.Infof("Exiting importState() successfully")
	return nil
}

func (s *snapshotImporter) createDBForNamespace(ns string) error {
	logger.Infof("Entering createDBForNamespace() with namespace: %s", ns)
	s.currentNs = ns
	var err error
	s.currentNsDB, err = s.vdb.getNamespaceDBHandle(ns)
	if err != nil {
		errMsg := errors.WithMessagef(err, "error while creating database for the namespace %s", ns)
		logger.Infof("Exiting createDBForNamespace() with error: %s", errMsg)
		return errMsg
	}
	logger.Infof("Exiting createDBForNamespace() successfully")
	return nil
}

func (s *snapshotImporter) storePendingDocs() error {
	logger.Infof("Entering storePendingDocs() with %d docs for namespace: %s", len(s.pendingDocsBatch), s.currentNs)
	if len(s.pendingDocsBatch) == 0 {
		logger.Infof("Exiting storePendingDocs() successfully - no pending docs")
		return nil
	}

	if err := s.currentNsDB.insertDocuments(s.pendingDocsBatch); err != nil {
		errMsg := errors.WithMessagef(
			err,
			"error while storing %d states associated with namespace %s",
			len(s.pendingDocsBatch), s.currentNs,
		)
		logger.Infof("Exiting storePendingDocs() with error: %s", errMsg)
		return errMsg
	}
	s.batchMemorySize = 0
	s.pendingDocsBatch = nil

	logger.Infof("Exiting storePendingDocs() successfully")
	return nil
}
