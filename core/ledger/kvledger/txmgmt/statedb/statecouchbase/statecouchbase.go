package statecouchbase

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
	"sort"
	"strings"
	"sync"
)

var logger = flogging.MustGetLogger("statecouchbase")

var maxDataImportBatchMemorySize = 2 * 1024 * 1024

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

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
	couchbaseInstance  *couchbaseInstance
	databases          map[string]*VersionedDB
	mux                sync.Mutex
	openCounts         uint64
	cache              *cache
	redoLoggerProvider *redoLoggerProvider
}

func NewVersionedDBProvider(config *ledger.CouchbaseConfig, sysNamespaces []string) (*VersionedDBProvider, error) {
	logger.Infof("Entering NewVersionedDBProvider()")
	couchbaseInstance, err := createCouchbaseInstance(config)
	if err != nil {
		logger.Infof("Exiting NewVersionedDBProvider() with error: %s", err)
		return nil, err
	}
	//if err := checkExpectedDataformatVersion(couchbaseInstance); err != nil {
	//	return nil, err
	//}
	// Todo add RedoLogPath to config
	p, err := newRedoLoggerProvider(config.RedoLogPath)
	if err != nil {
		couchbaseLogger.Infof("Error while initializing RedoLogger, exiting NewVersionedDBProvider() with error")
		return nil, err
	}

	// Add this config config.UserCacheSizeMBs
	cache := newCache(120, sysNamespaces)
	provider := &VersionedDBProvider{
		couchbaseInstance:  couchbaseInstance,
		databases:          make(map[string]*VersionedDB),
		mux:                sync.Mutex{},
		openCounts:         0,
		redoLoggerProvider: p,
		cache:              cache,
	}
	logger.Infof("Exiting NewVersionedDBProvider()")
	return provider, nil
}

func (provider *VersionedDBProvider) GetDBHandle(dbName string, namespaceProvider statedb.NamespaceProvider) (statedb.VersionedDB, error) {
	logger.Infof("Entering GetDBHandle() with database name: %s", dbName)
	provider.mux.Lock()
	defer provider.mux.Unlock()
	vdb := provider.databases[dbName]
	if vdb != nil {
		logger.Infof("Exiting GetDBHandle() with existing DB handle")
		return vdb, nil
	}

	var err error
	vdb, err = newVersionedDB(
		provider.couchbaseInstance,
		provider.redoLoggerProvider.newRedoLogger(dbName),
		dbName,
		provider.cache,
		namespaceProvider,
	)
	if err != nil {
		logger.Infof("Exiting GetDBHandle() with error: %s", err)
		return nil, err
	}
	provider.databases[dbName] = vdb
	logger.Infof("Exiting GetDBHandle() with new DB handle")
	return vdb, nil
}

func (provider *VersionedDBProvider) ImportFromSnapshot(dbName string, savepoint *version.Height, itr statedb.FullScanIterator) error {
	logger.Infof("Entering ImportFromSnapshot() with dbName: %s, savepoint height: %v", dbName, savepoint)
	metadataDB, err := createCouchbaseDatabase(provider.couchbaseInstance, constructMetadataDBName(dbName))
	if err != nil {
		errMsg := errors.WithMessagef(err, "error while creating the metadata database for channel %s", dbName)
		logger.Infof("Exiting ImportFromSnapshot() with error: %s", errMsg)
		return errMsg
	}

	vdb := &VersionedDB{
		chainName:         dbName,
		couchbaseInstance: provider.couchbaseInstance,
		metadataDB:        metadataDB,
		channelMetadata: &channelMetadata{
			ChannelName:      dbName,
			NamespaceDBsInfo: make(map[string]*namespaceDBInfo),
		},
		namespaceDBs: make(map[string]*couchbaseDatabase),
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

func (provider *VersionedDBProvider) BytesKeySupported() bool {
	logger.Infof("Entering BytesKeySupported()")
	logger.Infof("Exiting BytesKeySupported() with value: false")
	return false
}

func (provider *VersionedDBProvider) Close() {
	logger.Infof("Entering Close()")
	provider.redoLoggerProvider.close()
	logger.Infof("Exiting Close()")
}

func (provider *VersionedDBProvider) Drop(dbName string) error {
	logger.Infof("Entering Drop() with database name: %s", dbName)
	metadataDBName := constructMetadataDBName(dbName)
	couchbaseDatabase := couchbaseDatabase{couchbaseInstance: provider.couchbaseInstance, dbName: metadataDBName}
	dbExists := couchbaseDatabase.checkDatabaseExists()
	if !dbExists {
		// db does not exist
		logger.Infof("Exiting Drop(): database %s does not exist", dbName)
		return nil
	}

	metadataDB, err := createCouchbaseDatabase(provider.couchbaseInstance, metadataDBName)
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
		if err := dropDB(provider.couchbaseInstance, dbInfo.DBName); err != nil {
			logger.Errorw("Error dropping database", "channel", dbName, "namespace", dbInfo.Namespace, "error", err)
			logger.Infof("Exiting Drop() with error: %s", err)
			return err
		}
	}
	if err := dropDB(provider.couchbaseInstance, metadataDBName); err != nil {
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

	logger.Infof("Exiting Drop()")
	return nil
}

type VersionedDB struct {
	couchbaseInstance  *couchbaseInstance
	metadataDB         *couchbaseDatabase            // A database per channel to store metadata such as savepoint.
	chainName          string                        // The name of the chain/channel.
	namespaceDBs       map[string]*couchbaseDatabase // One database per namespace.
	channelMetadata    *channelMetadata              // Store channel name and namespaceDBInfo
	committedDataCache *versionsCache                // Used as a local cache during bulk processing of a block.
	verCacheLock       sync.RWMutex
	mux                sync.RWMutex
	redoLogger         *redoLogger
	cache              *cache
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(couchbaseInstance *couchbaseInstance, redoLogger *redoLogger, dbName string, cache *cache, nsProvider statedb.NamespaceProvider) (*VersionedDB, error) {
	logger.Infof("Entering newVersionedDB() with database name: %s", dbName)
	// CreateCouchDatabase creates a Couchbase database object, as well as the underlying database if it does not exist
	chainName := dbName
	dbName = constructMetadataDBName(dbName)

	metadataDB, err := createCouchbaseDatabase(couchbaseInstance, dbName)
	if err != nil {
		logger.Infof("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}
	namespaceDBMap := make(map[string]*couchbaseDatabase)
	vdb := &VersionedDB{
		couchbaseInstance:  couchbaseInstance,
		metadataDB:         metadataDB,
		chainName:          chainName,
		namespaceDBs:       namespaceDBMap,
		cache:              cache,
		redoLogger:         redoLogger,
		committedDataCache: newVersionCache(),
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
		if err := vdb.ApplyUpdates(redologRecord.UpdateBatch, redologRecord.Version); err != nil {
			logger.Infof("Exiting newVersionedDB() with error: %s", err)
			return nil, err
		}
	}
	logger.Infof("Exiting newVersionedDB() successfully")
	logger.Infof("Exiting newVersionedDB()")
	return vdb, nil
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
		logger.Infof("Exiting initChannelMetadata()")
		return vdb.writeChannelMetadata()
	}

	// read stored channelMetadata from an existing DB
	var err error
	vdb.channelMetadata, err = vdb.readChannelMetadata()
	if vdb.channelMetadata != nil || err != nil {
		logger.Infof("Exiting initChannelMetadata() with error: %s", err)
		return err
	}

	// channelMetadata is not present - this is the case when opening older dbs (e.g., v2.0/v2.1) for the first time
	// create channelMetadata and build NamespaceDBInfo mapping retroactively
	vdb.channelMetadata = &channelMetadata{
		ChannelName:      vdb.chainName,
		NamespaceDBsInfo: make(map[string]*namespaceDBInfo),
	}
	// retrieve existing DB names
	dbNames, err := vdb.couchbaseInstance.retrieveApplicationDBNames()
	if err != nil {
		logger.Infof("Exiting initChannelMetadata() with error: %s", err)
		return err
	}
	existingDBNames := make(map[string]struct{}, len(dbNames))
	for _, dbName := range dbNames {
		existingDBNames[dbName] = struct{}{}
	}
	// get namespaces and add a namespace to channelMetadata only if its DB name already exists
	namespaces, err := namespaceProvider.PossibleNamespaces(vdb)
	if err != nil {
		logger.Infof("Exiting initChannelMetadata() with error: %s", err)
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
	logger.Infof("Exiting initChannelMetadata()")
	return vdb.writeChannelMetadata()
}

// readChannelMetadata returns channel metadata stored in metadataDB
func (vdb *VersionedDB) readChannelMetadata() (*channelMetadata, error) {
	logger.Infof("Entering readChannelMetadata()")
	metadata, err := readChannelMetadata(vdb.metadataDB)
	if err != nil {
		logger.Infof("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting readChannelMetadata()")
	return metadata, nil
}

// retrieveApplicationDBNames returns all the application database names in the couch instance
func (couchbaseInstance *couchbaseInstance) retrieveApplicationDBNames() ([]string, error) {
	logger.Infof("Entering retrieveApplicationDBNames()")
	var applicationsDBNames []string
	for _, d := range getAllDatabases(couchbaseInstance) {
		if !isCouchbaseSystemDBName(d) {
			applicationsDBNames = append(applicationsDBNames, d)
		}
	}
	logger.Infof("Exiting retrieveApplicationDBNames() with %d database names", len(applicationsDBNames))
	return applicationsDBNames, nil
}

func isCouchbaseSystemDBName(name string) bool {
	logger.Infof("Entering isCouchbaseSystemDBName() with name: %s", name)
	result := strings.HasPrefix(name, "_")
	logger.Infof("Exiting isCouchbaseSystemDBName() with value: %t", result)
	return result
}

func readChannelMetadata(metadataDB *couchbaseDatabase) (*channelMetadata, error) {
	logger.Infof("Entering readChannelMetadata()")
	var err error
	couchbaseDoc, err := metadataDB.readDoc(channelMetadataDocID)
	if err != nil {
		logger.Errorf("Failed to read db name mapping data %s", err.Error())
		logger.Infof("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return nil
	if couchbaseDoc == nil {
		logger.Infof("Exiting readChannelMetadata() with nil value")
		return nil, nil
	}
	metadata, err := decodeChannelMetadata(couchbaseDoc)
	if err != nil {
		logger.Infof("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting readChannelMetadata()")
	return metadata, nil
}

// recordSavepoint records a savepoint in the metadata db for the channel.
func (vdb *VersionedDB) recordSavepoint(height *version.Height) error {
	// If a given height is nil, it denotes that we are committing pvt data of old blocks.
	// In this case, we should not store a savepoint for recovery. The lastUpdatedOldBlockList
	// in the pvtstore acts as a savepoint for pvt data.
	if height == nil {
		return nil
	}
	savepointCouchbaseDoc, err := encodeSavepoint(height)
	if err != nil {
		return err
	}
	err = vdb.metadataDB.saveDoc(savepointDocID, savepointCouchbaseDoc)
	if err != nil {
		logger.Errorf("Failed to save the savepoint to DB %s", err.Error())
		return err
	}
	return nil
}

func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	logger.Infof("Entering GetVersion() with namespace: %s, key: %s", namespace, key)

	version, keyFound := vdb.GetCachedVersion(namespace, key)
	if keyFound {
		logger.Infof("Exiting GetVersion() with cached version: %v", version)
		return version, nil
	}

	vv, err := vdb.GetState(namespace, key)
	if err != nil {
		logger.Infof("Exiting GetVersion() with error: %s", err)
		return nil, err
	}
	if vv == nil {
		logger.Infof("Exiting GetVersion() with nil version")
		return nil, nil
	}
	logger.Infof("Exiting GetVersion() with version: %v", vv.Version)
	return vv.Version, nil
}

func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	logger.Infof("Entering GetStateMultipleKeys() with namespace: %s, keys count: %d", namespace, len(keys))
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

func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	logger.Infof("Entering GetStateRangeScanIterator() with namespace: %q, startKey: %q, endKey: %q", namespace, startKey, endKey)
	// I honor internal limit
	result, err := vdb.GetStateRangeScanIteratorWithPagination(namespace, startKey, endKey, 0)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIterator() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetStateRangeScanIterator()")
	return result, nil
}

func (vdb *VersionedDB) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (statedb.QueryResultsIterator, error) {
	logger.Infof("Entering GetStateRangeScanIteratorWithPagination() with namespace: %q, startKey: %q, endKey: %q, pageSize: %d", namespace, startKey, endKey, pageSize)
	internalQueryLimit := vdb.couchbaseInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	scanner, err := newQueryScanner(namespace, db, "", internalQueryLimit, pageSize, "", startKey, endKey, true)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetStateRangeScanIteratorWithPagination()")
	return scanner, nil
}

func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	logger.Infof("Entering ExecuteQuery() with namespace: %s, query: %s", namespace, query)
	internalQueryLimit := vdb.couchbaseInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	//query, err = populateQuery(query, internalQueryLimit, "", db)
	//if err != nil {
	//	logger.Errorf("Error calling applyAdditionalQueryOptions(): %s", err.Error())
	//	logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
	//	return nil, err
	//}
	scanner, err := newQueryScanner(namespace, db, query, internalQueryLimit, 0, "", "", "", false)
	if err != nil {
		logger.Infof("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting ExecuteQuery()")
	return scanner, nil
}

func (vdb *VersionedDB) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (statedb.QueryResultsIterator, error) {
	logger.Infof("Entering ExecuteQueryWithPagination() with namespace: %s, query: %s, bookmark: %s, pageSize: %d", namespace, query, bookmark, pageSize)
	logger.Infof("Couchbase does not support")
	//return nil, nil

	internalQueryLimit := vdb.couchbaseInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	//query, err = populateQuery(query, internalQueryLimit, bookmark, db)
	//if err != nil {
	//	logger.Errorf("Error calling applyAdditionalQueryOptions(): %s", err.Error())
	//	logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
	//	return nil, err
	//}
	scanner, err := newQueryScanner(namespace, db, query, internalQueryLimit, pageSize, bookmark, "", "", false)
	if err != nil {
		logger.Infof("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting ExecuteQueryWithPagination()")
	return scanner, nil
}

func (vdb *VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	logger.Infof("Entering ApplyUpdates() with height: %v", height)

	if height != nil && batch.ContainsPostOrderWrites {
		// height is passed nil when committing missing private data for previously committed blocks
		r := &redoRecord{
			UpdateBatch: batch,
			Version:     height,
		}
		if err := vdb.redoLogger.persist(r); err != nil {
			logger.Infof("Exiting ApplyUpdates() with error persisting to redoLogger: %s", err)
			return err
		}
	}

	// stage 1 - buildCommitters builds committers per namespace (per DB). Each committer transforms the
	// given batch in the form of underlying db and keep it in memory.
	committers, err := vdb.buildCommitters(batch)
	if err != nil {
		logger.Infof("Exiting applyUpdates() with error building committers: %s", err)
		return err
	}

	if err = vdb.executeCommitter(committers); err != nil {
		logger.Infof("Exiting applyUpdates() with error executing committers: %s", err)
		return err
	}

	// Stgae 3 - postCommitProcessing - flush and record savepoint.
	namespaces := batch.GetUpdatedNamespaces()
	if err := vdb.postCommitProcessing(committers, namespaces, height); err != nil {
		logger.Infof("Exiting applyUpdates() with error in post commit processing: %s", err)
		return err
	}

	return nil
}

func (vdb *VersionedDB) postCommitProcessing(committers []*committer, namespaces []string, height *version.Height) error {
	couchbaseLogger.Infof("Entering postCommitProcessing() with %d committers, height: %v", len(committers), height)
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
		couchbaseLogger.Errorf("Error during recordSavepoint: %s", err.Error())
		couchbaseLogger.Infof("Exiting postCommitProcessing() with recordSavepoint error: %s", err)
		return err
	}

	wg.Wait()
	select {
	case err := <-errChan:
		couchbaseLogger.Infof("Exiting postCommitProcessing() with error from cache update: %s", err)
		return errors.WithStack(err)
	default:
		couchbaseLogger.Infof("Exiting postCommitProcessing() successfully")
		return nil
	}
}

func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
	logger.Infof("Entering GetLatestSavePoint()")
	var err error
	couchbaseDoc, err := vdb.metadataDB.readDoc(savepointDocID)
	if err != nil {
		logger.Errorf("Failed to read savepoint data %s", err.Error())
		if strings.Contains(err.Error(), "document not found") == true {
			logger.Infof("Exiting GetLatestSavePoint() with nil height, from error block")
			return nil, nil
		}
		logger.Infof("Exiting GetLatestSavePoint() with error: %s", err)
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return height nil
	if couchbaseDoc == nil {
		logger.Infof("Exiting GetLatestSavePoint() with nil height")
		return nil, nil
	}
	height, err := decodeSavepoint(couchbaseDoc)
	if err != nil {
		logger.Infof("Exiting GetLatestSavePoint() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetLatestSavePoint() with height: %v", height)
	return height, nil
}

func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	logger.Infof("Entering ValidateKeyValue() with key: %s", key)
	err := validateKey(key)
	if err != nil {
		logger.Infof("Exiting ValidateKeyValue() with error: %s", err)
		return err
	}
	err = validateValue(value)
	if err != nil {
		logger.Infof("Exiting ValidateKeyValue() with error: %s", err)
		return err
	}
	logger.Infof("Exiting ValidateKeyValue()")
	return nil
}

func (vdb *VersionedDB) BytesKeySupported() bool {
	logger.Infof("Entering BytesKeySupported()")
	logger.Infof("Exiting BytesKeySupported() with value: false")
	return false
}

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
			logger.Infof("Exiting GetFullScanIterator() with error: %s", err)
			return nil, errors.WithMessagef(err, "failed to get database handle for the namespace %s", ns)
		}
		dbsToScan = append(dbsToScan, &namespaceDB{ns, db})
	}

	// the database which belong to an empty namespace contains
	// internal keys. The scanner must skip these keys.
	toSkipKeysFromEmptyNs := map[string]bool{
		savepointDocID:       true,
		channelMetadataDocID: true,
	}
	scanner, err := newDBsScanner(dbsToScan, vdb.couchbaseInstance.internalQueryLimit(), toSkipKeysFromEmptyNs)
	if err != nil {
		logger.Infof("Exiting GetFullScanIterator() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting GetFullScanIterator()")
	return scanner, nil
}

func (vdb *VersionedDB) Open() error {
	logger.Infof("Entering Open()")
	//TODO implement me
	//panic("implement me")
	logger.Infof("Exiting Open()")
	return nil
}

func (vdb *VersionedDB) Close() {
	logger.Infof("Entering Close()")
	//TODO implement me, maybe close the couchbaseInstance inside ? Nope.
	//panic("implement me")
	logger.Infof("Exiting Close()")
}

// writeChannelMetadata saves channel metadata to metadataDB
func (vdb *VersionedDB) writeChannelMetadata() error {
	logger.Infof("Entering writeChannelMetadata()")
	err := vdb.metadataDB.saveDoc(channelMetadataDocID, vdb.channelMetadata)
	if err != nil {
		logger.Infof("Exiting writeChannelMetadata() with error: %s", err)
		return err
	}
	logger.Infof("Exiting writeChannelMetadata()")
	return nil
}

// getNamespaceDBHandle gets the handle to a named chaincode database
func (vdb *VersionedDB) getNamespaceDBHandle(namespace string) (*couchbaseDatabase, error) {
	logger.Infof("Entering getNamespaceDBHandle() with namespace: %s", namespace)
	vdb.mux.RLock()
	db := vdb.namespaceDBs[namespace]
	vdb.mux.RUnlock()
	if db != nil {
		logger.Infof("Exiting getNamespaceDBHandle() with existing database handle")
		return db, nil
	}
	namespaceDBName := constructNamespaceDBName(vdb.chainName, namespace)
	vdb.mux.Lock()
	defer vdb.mux.Unlock()

	db = vdb.namespaceDBs[namespace]
	if db != nil {
		logger.Infof("Exiting getNamespaceDBHandle() with existing database handle")
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
	db, err = createCouchbaseDatabase(vdb.couchbaseInstance, namespaceDBName)
	if err != nil {
		logger.Infof("Exiting getNamespaceDBHandle() with error: %s", err)
		return nil, err
	}
	vdb.namespaceDBs[namespace] = db
	logger.Infof("Exiting getNamespaceDBHandle() with new database handle")
	return db, nil
}

func (vdb *VersionedDB) readFromDB(namespace, key string) (*keyValue, error) {
	logger.Infof("Entering readFromDB() with namespace: %s, key: %s", namespace, key)
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	if err := validateKey(key); err != nil {
		logger.Infof("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	couchbaseDoc, err := db.readDoc(key)
	if err != nil {
		logger.Infof("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	if couchbaseDoc == nil {
		logger.Infof("Exiting readFromDB() with nil document")
		return nil, nil
	}
	kv, err := couchbaseDocToKeyValue(couchbaseDoc)
	if err != nil {
		logger.Infof("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting readFromDB() with keyValue")
	return kv, nil
}

func (vdb *VersionedDB) GetState(namespace, key string) (*statedb.VersionedValue, error) {
	logger.Infof("Entering GetState() with namespace: %s, key: %s", namespace, key)
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
	kv, err := vdb.readFromDB(namespace, key)

	if kv == nil {
		logger.Infof("Exiting GetState() with nil value")
		return nil, nil
	}

	if err != nil {
		logger.Infof("Exiting GetState() with error: %s", err)
		return nil, err
	}

	if cacheEnabled {
		cacheValue := constructCacheValue(kv.VersionedValue)
		if err := vdb.cache.putState(vdb.chainName, namespace, key, cacheValue); err != nil {
			logger.Infof("Exiting GetState() with error storing in cache: %s", err)
			return nil, err
		}
	}
	logger.Infof("Exiting GetState()")
	//logger.Infof("Exiting GetState() with value: %v", kv.VersionedValue)
	return kv.VersionedValue, nil
}

////////////// SUPPORT_FOR_INDEXES///////////////////////////////

// ProcessIndexesForChaincodeDeploy creates indexes for a specified namespace
func (vdb *VersionedDB) ProcessIndexesForChaincodeDeploy(namespace string, indexFilesData map[string][]byte) error {
	logger.Infof("Entering ProcessIndexesForChaincodeDeploy() with namespace: %s, %d index files", namespace, len(indexFilesData))
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		logger.Infof("Exiting ProcessIndexesForChaincodeDeploy() with error: %s", err)
		return err
	}

	var indexFilesName []string
	for fileName := range indexFilesData {
		indexFilesName = append(indexFilesName, fileName)
	}
	sort.Strings(indexFilesName)
	for _, fileName := range indexFilesName {
		var indexData IndexData
		err = json.Unmarshal(indexFilesData[fileName], &indexData)
		if err != nil {
			logger.Errorf("error unmarshalling index data from file [%s] for chaincode [%s] on channel [%s]: %+v",
				fileName, namespace, vdb.chainName, err)
			continue
		}
		query := indexData.Index
		query, _, _ = populateQuery(query, 0, "", db)
		_, err := db.queryDocuments(query)
		if err != nil {
			return err
		}

		//switch {
		//case err != nil:
		//	logger.Errorf("error creating index from file [%s] for chaincode [%s] on channel [%s]: %+v",
		//		fileName, namespace, vdb.chainName, err)
		//default:
		//	logger.Infof("successfully submitted index creation request present in the file [%s] for chaincode [%s] on channel [%s]",
		//		fileName, namespace, vdb.chainName)
		//}
	}
	logger.Infof("Exiting ProcessIndexesForChaincodeDeploy() successfully")
	return nil
}

// GetDBType returns the hosted stateDB
func (vdb *VersionedDB) GetDBType() string {
	logger.Infof("Entering GetDBType()")
	logger.Infof("Exiting GetDBType() with value: couchbase")
	return "couchbase"
}

//////////////SUPPORT_FOR_BULK_OPTIMIZABLE///////////////////////

func (vdb *VersionedDB) LoadCommittedVersions(keys []*statedb.CompositeKey) error {
	logger.Infof("Entering LoadCommittedVersions() with %d keys", len(keys))
	missingKeys := make(map[string][]gocb.BulkOp)
	committedDataCache := newVersionCache()
	for _, compositeKey := range keys {
		ns, key := compositeKey.Namespace, compositeKey.Key
		committedDataCache.setVerAndRev(ns, key, nil)
		logger.Infof("Load into version cache: %s~%s", ns, key)

		if !vdb.cache.enabled(ns) {
			missingKeys[ns] = append(missingKeys[ns],
				&gocb.GetOp{
					ID: key,
				})
			continue
		}
		cv, err := vdb.cache.getState(vdb.chainName, ns, key)
		if err != nil {
			logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
			return err
		}
		if cv == nil {
			missingKeys[ns] = append(missingKeys[ns], &gocb.GetOp{
				ID: key,
			})
			continue
		}
		vv, err := constructVersionedValue(cv)
		if err != nil {
			logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
			return err
		}
		committedDataCache.setVerAndRev(ns, key, vv.Version)
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
			kv, err := couchbaseDocToKeyValue(keyMetadata)
			if err != nil {
				logger.Infof("Exiting LoadCommittedVersions() with error: %s", err)
				return err
			}
			committedDataCache.setVerAndRev(ns, kv.key, kv.Version)
		}
	}
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = committedDataCache
	logger.Infof("Exiting LoadCommittedVersions() successfully")
	return nil
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

// ClearCachedVersions clears committedVersions and revisionNumbers
func (vdb *VersionedDB) ClearCachedVersions() {
	logger.Infof("Entering ClearCachedVersions()")
	logger.Infof("Clear Cache")
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = newVersionCache()
	logger.Infof("Exiting ClearCachedVersions()")
}

////////////// QUERY_SCANNER_SECTION_STARTS//////////////////////

type queryScanner struct {
	namespace       string
	db              *couchbaseDatabase
	queryDefinition *queryDefinition
	paginationInfo  *paginationInfo
	resultsInfo     *resultsInfo
	exhausted       bool
	offset          int32
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
	results              []*couchbaseDoc
}

func (scanner *queryScanner) Next() (*statedb.VersionedKV, error) {
	logger.Infof("Entering queryScanner.Next()")
	doc, err := scanner.next()
	if err != nil {
		logger.Infof("Exiting queryScanner.Next() with error: %s", err)
		return nil, err
	}
	if doc == nil {
		logger.Infof("Exiting queryScanner.Next() with nil document")
		return nil, nil
	}
	kv, err := couchbaseDocToKeyValue(doc)
	if err != nil {
		logger.Infof("Exiting queryScanner.Next() with error: %s", err)
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
	logger.Infof("Exiting queryScanner.Next() with key: %s", kv.key)
	return result, nil
}

func (scanner *queryScanner) getNextStateRangeScanResults() error {
	logger.Infof("Entering getNextStateRangeScanResults()")
	queryLimit := scanner.queryDefinition.internalQueryLimit
	if scanner.paginationInfo.requestedLimit > 0 {
		moreResultsNeeded := scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
		if moreResultsNeeded < scanner.queryDefinition.internalQueryLimit {
			queryLimit = moreResultsNeeded
		}
	}
	queryResult, nextStartKey, offset, err := scanner.db.readDocRange(scanner.queryDefinition.startKey, scanner.queryDefinition.endKey, queryLimit, scanner.offset)
	if err != nil {
		logger.Infof("Exiting getNextStateRangeScanResults() with error: %s", err)
		return err
	}
	logger.Infof("Size of queryResult: %d", len(queryResult))
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.cursor = 0
	if isEffectivelyEmpty(scanner.queryDefinition.startKey) && isEffectivelyEmpty(scanner.queryDefinition.endKey) {
		if offset == -1 {
			couchbaseLogger.Infof("Exiting getNextStateRangeScanResults() with offset (Exhausted): %d", offset)
			scanner.exhausted = true
		}
		scanner.offset = offset
	} else {
		if scanner.queryDefinition.endKey == nextStartKey {
			// as we always set inclusive_end=false to match the behavior of
			// goleveldb iterator, it is safe to mark the scanner as exhausted
			couchbaseLogger.Infof("Exiting getNextStateRangeScanResults() with endKey (Exhausted): %s", nextStartKey)
			scanner.exhausted = true
			// we still need to update the startKey as it is returned as bookmark
		}
		scanner.queryDefinition.startKey = nextStartKey
	}
	logger.Infof("Exiting getNextStateRangeScanResults()")
	return nil
}

//func rangeScanFilterCouchbaseInternalDocs(db *couchbaseDatabase,
//	startKey, endKey string, queryLimit int32,
//) ([]*queryResult, string, error) {
//	logger.Infof("Entering rangeScanFilterCouchbaseInternalDocs() with startKey: %q, endKey: %q, queryLimit: %d", startKey, endKey, queryLimit)
//	var finalResults []*queryResult
//	var finalNextStartKey string
//	for {
//		results, nextStartKey, err := db.readDocRange(startKey, endKey, queryLimit)
//		if err != nil {
//			logger.Infof("Error calling ReadDocRange(): %s\n", err.Error())
//			logger.Infof("Exiting rangeScanFilterCouchbaseInternalDocs() with error: %s", err)
//			return nil, "", err
//		}
//		var filteredResults []*queryResult
//		for _, doc := range results {
//			if !isCouchbaseInternalKey(doc.id) {
//				filteredResults = append(filteredResults, doc)
//			}
//		}
//
//		finalResults = append(finalResults, filteredResults...)
//		finalNextStartKey = nextStartKey
//		queryLimit = int32(len(results) - len(filteredResults))
//		if queryLimit == 0 || finalNextStartKey == "" {
//			break
//		}
//		startKey = finalNextStartKey
//	}
//	var err error
//	for i := 0; isCouchbaseInternalKey(finalNextStartKey); i++ {
//		_, finalNextStartKey, err = db.readDocRange(finalNextStartKey, endKey, 1)
//		logger.Infof("i=%d, finalNextStartKey=%s", i, finalNextStartKey)
//		if err != nil {
//			logger.Infof("Exiting rangeScanFilterCouchbaseInternalDocs() with error: %s", err)
//			return nil, "", err
//		}
//	}
//	logger.Infof("Exiting rangeScanFilterCouchbaseInternalDocs() with %d results", len(finalResults))
//	return finalResults, finalNextStartKey, nil
//}

func (scanner *queryScanner) next() (*couchbaseDoc, error) {
	logger.Infof("Entering queryScanner.next()")
	couchbaseLogger.Infof("QueryScanner.next() %v,", scanner.resultsInfo)
	couchbaseLogger.Infof("QueryScanner.next() cursor: %d, requestedLimit: %d, bookmark: %s,", scanner.paginationInfo.cursor, scanner.paginationInfo.requestedLimit, scanner.paginationInfo.bookmark)
	if len(scanner.resultsInfo.results) == 0 {
		logger.Infof("Exiting queryScanner.next() with nil document (no results)")
		return nil, nil
	}
	scanner.paginationInfo.cursor++
	if scanner.paginationInfo.cursor >= scanner.queryDefinition.internalQueryLimit {
		if scanner.exhausted {
			logger.Infof("Exiting queryScanner.next() with nil document (exhausted)")
			return nil, nil
		}
		var err error
		if scanner.queryDefinition.query != "" {
			err = scanner.executeQueryWithBookmark()
		} else {
			err = scanner.getNextStateRangeScanResults()
		}
		if err != nil {
			logger.Infof("Exiting queryScanner.next() with error: %s", err)
			return nil, err
		}
		if len(scanner.resultsInfo.results) == 0 {
			logger.Infof("Exiting queryScanner.next() with nil document (no new results)")
			return nil, nil
		}
	}
	if scanner.paginationInfo.cursor >= int32(len(scanner.resultsInfo.results)) {
		logger.Infof("Exiting queryScanner.next() with nil document (cursor beyond results)")
		return nil, nil
	}
	result := scanner.resultsInfo.results[scanner.paginationInfo.cursor]
	logger.Infof("Exiting queryScanner.next() with document: %v", result)
	return result, nil
}

func (scanner *queryScanner) Close() {
	logger.Infof("Entering queryScanner.Close()")
	logger.Infof("Exiting queryScanner.Close()")
}

func (scanner *queryScanner) GetBookmarkAndClose() string {
	logger.Infof("Entering queryScanner.GetBookmarkAndClose()")
	retval := ""
	if scanner.queryDefinition.query != "" {
		retval = scanner.paginationInfo.bookmark
	} else {
		retval = scanner.queryDefinition.startKey
	}
	scanner.Close()
	logger.Infof("Exiting queryScanner.GetBookmarkAndClose()")
	return retval
}

func newQueryScanner(namespace string, db *couchbaseDatabase, query string, internalQueryLimit,
	limit int32, bookmark, startKey, endKey string, honorLimitBookmark bool) (*queryScanner, error) {
	logger.Infof("Entering newQueryScanner() with namespace: %s, query: %s, limit: %d, bookmark: %s, startKey: %s, endKey: %s",
		namespace, query, limit, bookmark, startKey, endKey)
	scanner := &queryScanner{namespace, db, &queryDefinition{startKey, endKey, query, internalQueryLimit}, &paginationInfo{-1, limit, bookmark}, &resultsInfo{0, nil}, false, 0}
	var err error
	// query is defined, then execute the query and return the records and bookmark
	if scanner.queryDefinition.query != "" {
		err = scanner.executeQueryWithBookmark()
	} else {
		err = scanner.getNextStateRangeScanResults()
	}
	if err != nil {
		logger.Infof("Exiting newQueryScanner() with error: %s", err)
		return nil, err
	}
	scanner.paginationInfo.cursor = -1
	logger.Infof("Exiting newQueryScanner()")
	return scanner, nil
}

// executeQueryWithBookmark executes a "paging" query with a bookmark, this method allows a
// paged query without returning a new query iterator
func (scanner *queryScanner) executeQueryWithBookmark() error {
	logger.Infof("Entering executeQueryWithBookmark()")
	queryLimit := scanner.queryDefinition.internalQueryLimit
	if scanner.paginationInfo.requestedLimit > 0 {
		if scanner.paginationInfo.requestedLimit-scanner.resultsInfo.totalRecordsReturned < scanner.queryDefinition.internalQueryLimit {
			queryLimit = scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
		}
	}
	queryString, updatedBookmark, err := populateQuery(scanner.queryDefinition.query,
		queryLimit, scanner.paginationInfo.bookmark, scanner.db)
	if err != nil {
		logger.Infof("Error calling applyAdditionalQueryOptions(): %s\n", err.Error())
		logger.Infof("Exiting executeQueryWithBookmark() with error: %s", err)
		return err
	}
	queryResult, err := scanner.db.queryDocuments(queryString)
	if err != nil {
		logger.Infof("Error calling QueryDocuments(): %s\n", err.Error())
		logger.Infof("Exiting executeQueryWithBookmark() with error: %s", err)
		return err
	}
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.bookmark = updatedBookmark
	scanner.paginationInfo.cursor = 0
	logger.Infof("Exiting executeQueryWithBookmark() with bookmark: %s, queryResult: %v", updatedBookmark, queryResult)
	return nil
}

//func (scanner *queryScanner) getNextStateRangeScanResults() error {
//	queryLimit := scanner.queryDefinition.internalQueryLimit
//	if scanner.paginationInfo.requestedLimit > 0 {
//		moreResultsNeeded := scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
//		if moreResultsNeeded < scanner.queryDefinition.internalQueryLimit {
//			queryLimit = moreResultsNeeded
//		}
//	}
//	queryResult, nextStartKey, err := rangeScanFilterCouchInternalDocs(scanner.db,
//		scanner.queryDefinition.startKey, scanner.queryDefinition.endKey, queryLimit)
//	if err != nil {
//		return err
//	}
//	scanner.resultsInfo.results = queryResult
//	scanner.paginationInfo.cursor = 0
//	if scanner.queryDefinition.endKey == nextStartKey {
//		// as we always set inclusive_end=false to match the behavior of
//		// goleveldb iterator, it is safe to mark the scanner as exhausted
//		scanner.exhausted = true
//		// we still need to update the startKey as it is returned as bookmark
//	}
//	scanner.queryDefinition.startKey = nextStartKey
//	return nil
//}

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
	db *couchbaseDatabase
}

func newDBsScanner(dbsToScan []*namespaceDB, prefetchLimit int32, toSkipKeysFromEmptyNs map[string]bool) (*dbsScanner, error) {
	logger.Infof("Entering newDBsScanner() with %d databases to scan", len(dbsToScan))
	if len(dbsToScan) == 0 {
		logger.Infof("Exiting newDBsScanner() with nil scanner (no databases)")
		return nil, nil
	}
	s := &dbsScanner{
		dbs:                   dbsToScan,
		prefetchLimit:         prefetchLimit,
		toSkipKeysFromEmptyNs: toSkipKeysFromEmptyNs,
	}
	if err := s.beginNextDBScan(); err != nil {
		logger.Infof("Exiting newDBsScanner() with error: %s", err)
		return nil, err
	}
	logger.Infof("Exiting newDBsScanner()")
	return s, nil
}

func (s *dbsScanner) beginNextDBScan() error {
	logger.Infof("Entering beginNextDBScan()")
	dbUnderScan := s.dbs[s.nextDBToScanIndex]
	queryScanner, err := newQueryScanner(dbUnderScan.ns, dbUnderScan.db, "", s.prefetchLimit, 0, "", "", "", true)
	if err != nil {
		logger.Infof("Exiting beginNextDBScan() with error: %s", err)
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
	logger.Infof("Exiting beginNextDBScan()")
	return nil
}

// Next returns the key-values present in the namespaceDB. Once a namespaceDB
// is processed, it moves to the next namespaceDB till all are processed.
func (s *dbsScanner) Next() (*statedb.VersionedKV, error) {
	logger.Infof("Entering dbsScanner.Next()")
	if s == nil {
		logger.Infof("Exiting dbsScanner.Next() with nil value (scanner is nil)")
		return nil, nil
	}
	for {
		couchbaseDoc, err := s.resultItr.next()
		if err != nil {
			logger.Infof("Exiting dbsScanner.Next() with error: %s", err)
			return nil, errors.WithMessagef(
				err,
				"failed to retrieve the next entry from scanner associated with namespace %s",
				s.currentNamespace,
			)
		}
		if couchbaseDoc == nil {
			s.resultItr.Close()
			if len(s.dbs) <= s.nextDBToScanIndex {
				logger.Infof("Exiting dbsScanner.Next() with nil value (all databases processed)")
				break
			}
			if err := s.beginNextDBScan(); err != nil {
				logger.Infof("Exiting dbsScanner.Next() with error: %s", err)
				return nil, err
			}
			continue
		}
		if s.currentNamespace == "" {
			key, err := couchbaseDoc.key()
			if err != nil {
				logger.Infof("Exiting dbsScanner.Next() with error: %s", err)
				return nil, errors.WithMessagef(
					err,
					"failed to retrieve key from the couchdoc present in the empty namespace",
				)
			}
			if s.toSkipKeysFromEmptyNs[key] {
				continue
			}
		}
		kv, err := couchbaseDocToKeyValue(couchbaseDoc)
		if err != nil {
			logger.Infof("Exiting dbsScanner.Next() with error: %s", err)
			return nil, errors.WithMessagef(
				err,
				"failed to validate and retrieve fields from couch doc with id %s",
				kv.key,
			)
		}
		result := &statedb.VersionedKV{
			CompositeKey: &statedb.CompositeKey{
				Namespace: s.currentNamespace,
				Key:       kv.key,
			},
			VersionedValue: kv.VersionedValue,
		}
		logger.Infof("Exiting dbsScanner.Next() with key: %s", kv.key)
		return result, nil
	}
	logger.Infof("Exiting dbsScanner.Next() with nil value")
	return nil, nil
}

func (s *dbsScanner) Close() {
	logger.Infof("Entering dbsScanner.Close()")
	if s == nil {
		logger.Infof("Exiting dbsScanner.Close() (scanner is nil)")
		return
	}
	s.resultItr.Close()
	logger.Infof("Exiting dbsScanner.Close()")
}

type snapshotImporter struct {
	vdb              *VersionedDB
	itr              statedb.FullScanIterator
	currentNs        string
	currentNsDB      *couchbaseDatabase
	pendingDocsBatch []*couchbaseDoc
	batchMemorySize  int
}

func (s *snapshotImporter) importState() error {
	logger.Infof("Entering importState()")
	if s.itr == nil {
		logger.Infof("Exiting importState() (iterator is nil)")
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

		doc, err := keyValToCouchbaseDoc(
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
		s.batchMemorySize += len(*doc)

		// TODO populate with max batch update size.
		if s.batchMemorySize >= maxDataImportBatchMemorySize ||
			len(s.pendingDocsBatch) == 100 {
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
	logger.Infof("Exiting importState()")
	return nil
}

func (s *snapshotImporter) createDBForNamespace(ns string) error {
	logger.Infof("Entering createDBForNamespace() with namespace: %s", ns)
	s.currentNs = ns
	var err error
	s.currentNsDB, err = s.vdb.getNamespaceDBHandle(ns)
	if err != nil {
		logger.Infof("Exiting createDBForNamespace() with error: %s", err)
		return errors.WithMessagef(err, "error while creating database for the namespace %s", ns)
	}
	logger.Infof("Exiting createDBForNamespace()")
	return nil
}

func (s *snapshotImporter) storePendingDocs() error {
	logger.Infof("Entering storePendingDocs() with %d pending documents", len(s.pendingDocsBatch))
	if len(s.pendingDocsBatch) == 0 {
		logger.Infof("Exiting storePendingDocs() (no pending docs)")
		return nil
	}

	if err := s.currentNsDB.insertDocuments(s.pendingDocsBatch); err != nil {
		logger.Infof("Exiting storePendingDocs() with error: %s", err)
		return errors.WithMessagef(
			err,
			"error while storing %d states associated with namespace %s",
			len(s.pendingDocsBatch), s.currentNs,
		)
	}
	s.batchMemorySize = 0
	s.pendingDocsBatch = nil

	logger.Infof("Exiting storePendingDocs()")
	return nil
}
