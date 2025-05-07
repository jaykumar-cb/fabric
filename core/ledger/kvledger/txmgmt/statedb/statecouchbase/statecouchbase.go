package statecouchbase

import (
	"encoding/json"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"sort"
	"strings"
	"sync"

	"github.com/couchbase/gocb/v2"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
)

//// This is the debug logger, cmd + shift + f, search loggerd, cmd + / to uncomment all the dev loggers
//var  loggerd = flogging.MustGetLogger("statecouchbase")

// This is the prod logger
var loggerp = flogging.MustGetLogger("statecouchbase")

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

func NewVersionedDBProvider(config *ledger.CouchbaseConfig, metricsProvider metrics.Provider, sysNamespaces []string) (*VersionedDBProvider, error) {
	// loggerd.Debugf("Entering NewVersionedDBProvider()")
	loggerp.Debugf("constructing Couchbase VersionedDBProvider")
	couchbaseInstance, err := createCouchbaseInstance(config)
	if err != nil {
		// loggerd.Debugf("Exiting NewVersionedDBProvider() with error: %s", err)
		return nil, err
	}
	//if err := checkExpectedDataformatVersion(couchbaseInstance); err != nil {
	//	return nil, err
	//}
	p, err := newRedoLoggerProvider(config.RedoLogPath)
	if err != nil {
		// loggerd.Debugf("Error while initializing RedoLogger, exiting NewVersionedDBProvider() with error: %s", err)
		return nil, err
	}
	cache := newCache(config.UserCacheSizeMBs, sysNamespaces)
	provider := &VersionedDBProvider{
		couchbaseInstance:  couchbaseInstance,
		databases:          make(map[string]*VersionedDB),
		mux:                sync.Mutex{},
		openCounts:         0,
		redoLoggerProvider: p,
		cache:              cache,
	}
	// loggerd.Debugf("Exiting NewVersionedDBProvider()")
	return provider, nil
}

func (provider *VersionedDBProvider) GetDBHandle(dbName string, namespaceProvider statedb.NamespaceProvider) (statedb.VersionedDB, error) {
	// loggerd.Debugf("Entering GetDBHandle() with database name: %s", dbName)
	provider.mux.Lock()
	defer provider.mux.Unlock()
	vdb := provider.databases[dbName]
	if vdb != nil {
		// loggerd.Debugf("Exiting GetDBHandle() with existing DB handle")
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
		// loggerd.Debugf("Exiting GetDBHandle() with error: %s", err)
		return nil, err
	}
	provider.databases[dbName] = vdb
	// loggerd.Debugf("Exiting GetDBHandle() with new DB handle")
	return vdb, nil
}

func (provider *VersionedDBProvider) ImportFromSnapshot(dbName string, savepoint *version.Height, itr statedb.FullScanIterator) error {
	// loggerd.Debugf("Entering ImportFromSnapshot() with dbName: %s, savepoint height: %v", dbName, savepoint)
	metadataDB, err := createCouchbaseDatabase(provider.couchbaseInstance, constructMetadataDBName(dbName))
	if err != nil {
		errMsg := errors.WithMessagef(err, "error while creating the metadata database for channel %s", dbName)
		// loggerd.Debugf("Exiting ImportFromSnapshot() with error: %s", errMsg)
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
		// loggerd.Debugf("Exiting ImportFromSnapshot() with error: %s", errMsg)
		return errMsg
	}

	s := &snapshotImporter{
		vdb: vdb,
		itr: itr,
	}
	if err := s.importState(); err != nil {
		// loggerd.Debugf("Exiting ImportFromSnapshot() with error: %s", err)
		return err
	}

	err = vdb.recordSavepoint(savepoint)
	if err != nil {
		// loggerd.Debugf("Exiting ImportFromSnapshot() with error: %s", err)
		return err
	}

	// loggerd.Debugf("Exiting ImportFromSnapshot() successfully")
	return nil
}

func (provider *VersionedDBProvider) BytesKeySupported() bool {
	// loggerd.Debugf("Entering BytesKeySupported()")
	// loggerd.Debugf("Exiting BytesKeySupported() with value: false")
	return false
}

func (provider *VersionedDBProvider) Close() {
	// loggerd.Debugf("Entering Close()")
	provider.redoLoggerProvider.close()
	// loggerd.Debugf("Exiting Close()")
}

func (provider *VersionedDBProvider) Drop(dbName string) error {
	// loggerd.Debugf("Entering Drop() with database name: %s", dbName)
	metadataDBName := constructMetadataDBName(dbName)
	couchbaseDatabase := couchbaseDatabase{couchbaseInstance: provider.couchbaseInstance, dbName: metadataDBName}
	dbExists, err := couchbaseDatabase.checkDatabaseExists()
	if err != nil {
		return err
	}

	if !dbExists {
		// db does not exist
		// loggerd.Debugf("Exiting Drop(): database %s does not exist", dbName)
		return nil
	}

	metadataDB, err := createCouchbaseDatabase(provider.couchbaseInstance, metadataDBName)
	if err != nil {
		// loggerd.Debugf("Exiting Drop() with error: %s", err)
		return err
	}
	channelMetadata, err := readChannelMetadata(metadataDB)
	if err != nil {
		// loggerd.Debugf("Exiting Drop() with error: %s", err)
		return err
	}

	for _, dbInfo := range channelMetadata.NamespaceDBsInfo {
		// do not drop metadataDB until all other dbs are dropped
		if dbInfo.DBName == metadataDBName {
			continue
		}
		if err := dropDB(provider.couchbaseInstance, dbInfo.DBName); err != nil {
			loggerp.Errorw("Error dropping database", "channel", dbName, "namespace", dbInfo.Namespace, "error", err)
			return err
		}
	}
	if err := dropDB(provider.couchbaseInstance, metadataDBName); err != nil {
		loggerp.Errorw("Error dropping metadataDB", "channel", dbName, "error", err)
		return err
	}

	delete(provider.databases, dbName)

	err = provider.redoLoggerProvider.leveldbProvider.Drop(dbName)
	if err != nil {
		// loggerd.Debugf("Exiting Drop() with error: %s", err)
		return err
	}

	// loggerd.Debugf("Exiting Drop()")
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
	// loggerd.Debugf("Entering newVersionedDB() with database name: %s", dbName)
	// CreateCouchDatabase creates a Couchbase database object, as well as the underlying database if it does not exist
	chainName := dbName
	dbName = constructMetadataDBName(dbName)

	metadataDB, err := createCouchbaseDatabase(couchbaseInstance, dbName)
	if err != nil {
		// loggerd.Debugf("Exiting newVersionedDB() with error: %s", err)
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

	// loggerd.Debugf("chain [%s]: checking for redolog record", chainName)
	redologRecord, err := redoLogger.load()
	if err != nil {
		// loggerd.Debugf("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}

	savepoint, err := vdb.GetLatestSavePoint()
	if err != nil {
		// loggerd.Debugf("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}

	isNewDB := savepoint == nil
	if err = vdb.initChannelMetadata(isNewDB, nsProvider); err != nil {
		// loggerd.Debugf("Exiting newVersionedDB() with error: %s", err)
		return nil, err
	}

	// in normal circumstances, redolog is expected to be either equal to the last block
	// committed to the statedb or one ahead (in the event of a crash). However, either of
	// these or both could be nil on first time start (fresh start/rebuild)
	if redologRecord == nil || savepoint == nil {
		// loggerd.Debugf("chain [%s]: No redo-record or save point present", chainName)
		// loggerd.Debugf("Exiting newVersionedDB() successfully")
		return vdb, nil
	}

	// loggerd.Debugf("chain [%s]: save point = %#v, version of redolog record = %#v", chainName, savepoint, redologRecord.Version)

	if redologRecord.Version.BlockNum-savepoint.BlockNum == 1 {
		// loggerd.Debugf("chain [%s]: Re-applying last batch", chainName)
		if err := vdb.ApplyUpdates(redologRecord.UpdateBatch, redologRecord.Version); err != nil {
			// loggerd.Debugf("Exiting newVersionedDB() with error: %s", err)
			return nil, err
		}
	}
	// loggerd.Debugf("Exiting newVersionedDB() successfully")
	// loggerd.Debugf("Exiting newVersionedDB()")
	return vdb, nil
}

// initChannelMetadata initizlizes channelMetadata and build NamespaceDBInfo mapping if not present
func (vdb *VersionedDB) initChannelMetadata(isNewDB bool, namespaceProvider statedb.NamespaceProvider) error {
	// loggerd.Debugf("Entering initChannelMetadata() with isNewDB: %t", isNewDB)
	// create channelMetadata with empty NamespaceDBInfo mapping for a new DB
	if isNewDB {
		vdb.channelMetadata = &channelMetadata{
			ChannelName:      vdb.chainName,
			NamespaceDBsInfo: make(map[string]*namespaceDBInfo),
		}
		// loggerd.Debugf("Exiting initChannelMetadata()")
		return vdb.writeChannelMetadata()
	}

	// read stored channelMetadata from an existing DB
	var err error
	vdb.channelMetadata, err = vdb.readChannelMetadata()
	if vdb.channelMetadata != nil || err != nil {
		// loggerd.Debugf("Exiting initChannelMetadata() with error: %s", err)
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
		// loggerd.Debugf("Exiting initChannelMetadata() with error: %s", err)
		return err
	}
	existingDBNames := make(map[string]struct{}, len(dbNames))
	for _, dbName := range dbNames {
		existingDBNames[dbName] = struct{}{}
	}
	// get namespaces and add a namespace to channelMetadata only if its DB name already exists
	namespaces, err := namespaceProvider.PossibleNamespaces(vdb)
	if err != nil {
		// loggerd.Debugf("Exiting initChannelMetadata() with error: %s", err)
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
	// loggerd.Debugf("Exiting initChannelMetadata()")
	return vdb.writeChannelMetadata()
}

// readChannelMetadata returns channel metadata stored in metadataDB
func (vdb *VersionedDB) readChannelMetadata() (*channelMetadata, error) {
	// loggerd.Debugf("Entering readChannelMetadata()")
	metadata, err := readChannelMetadata(vdb.metadataDB)
	if err != nil {
		// loggerd.Debugf("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting readChannelMetadata()")
	return metadata, nil
}

// retrieveApplicationDBNames returns all the application database names in the couch instance
func (couchbaseInstance *couchbaseInstance) retrieveApplicationDBNames() ([]string, error) {
	// loggerd.Debugf("Entering retrieveApplicationDBNames()")
	var applicationsDBNames []string
	allDatabases, err := getAllDatabases(couchbaseInstance)
	if err != nil {
		return nil, err
	}
	for _, d := range allDatabases {
		if !isCouchbaseSystemDBName(d) {
			applicationsDBNames = append(applicationsDBNames, d)
		}
	}
	// loggerd.Debugf("Exiting retrieveApplicationDBNames() with %d database names", len(applicationsDBNames))
	return applicationsDBNames, nil
}

func isCouchbaseSystemDBName(name string) bool {
	// loggerd.Debugf("Entering isCouchbaseSystemDBName() with name: %s", name)
	result := strings.HasPrefix(name, "_")
	// loggerd.Debugf("Exiting isCouchbaseSystemDBName() with value: %t", result)
	return result
}

func readChannelMetadata(metadataDB *couchbaseDatabase) (*channelMetadata, error) {
	// loggerd.Debugf("Entering readChannelMetadata()")
	var err error
	couchbaseDoc, err := metadataDB.readDoc(channelMetadataDocID)
	if err != nil {
		loggerp.Errorf("Failed to read db name mapping data %s", err.Error())
		// loggerd.Debugf("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return nil
	if couchbaseDoc == nil {
		// loggerd.Debugf("Exiting readChannelMetadata() with nil value")
		return nil, nil
	}
	metadata, err := decodeChannelMetadata(couchbaseDoc)
	if err != nil {
		// loggerd.Debugf("Exiting readChannelMetadata() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting readChannelMetadata()")
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
	(*savepointCouchbaseDoc)[idField] = savepointDocID
	err = vdb.metadataDB.saveDoc(savepointDocID, savepointCouchbaseDoc)
	if err != nil {
		loggerp.Errorf("Failed to save the savepoint to DB %s", err.Error())
		return err
	}
	return nil
}

func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	// loggerd.Debugf("Entering GetVersion() with namespace: %s, key: %s", namespace, key)

	version, keyFound := vdb.GetCachedVersion(namespace, key)
	if keyFound {
		// loggerd.Debugf("Exiting GetVersion() with cached version: %v", version)
		return version, nil
	}

	vv, err := vdb.GetState(namespace, key)
	if err != nil {
		// loggerd.Debugf("Exiting GetVersion() with error: %s", err)
		return nil, err
	}
	if vv == nil {
		// loggerd.Debugf("Exiting GetVersion() with nil version")
		return nil, nil
	}
	// loggerd.Debugf("Exiting GetVersion() with version: %v", vv.Version)
	return vv.Version, nil
}

func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	// loggerd.Debugf("Entering GetStateMultipleKeys() with namespace: %s, keys count: %d", namespace, len(keys))
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			// loggerd.Debugf("Exiting GetStateMultipleKeys() with error: %s", err)
			return nil, err
		}
		vals[i] = val
	}
	// loggerd.Debugf("Exiting GetStateMultipleKeys() with %d values", len(vals))
	return vals, nil
}

func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	// loggerd.Debugf("Entering GetStateRangeScanIterator() with namespace: %q, startKey: %q, endKey: %q", namespace, startKey, endKey)
	// I honor internal limit
	result, err := vdb.GetStateRangeScanIteratorWithPagination(namespace, startKey, endKey, 0)
	if err != nil {
		// loggerd.Debugf("Exiting GetStateRangeScanIterator() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting GetStateRangeScanIterator()")
	return result, nil
}

func (vdb *VersionedDB) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (statedb.QueryResultsIterator, error) {
	// loggerd.Debugf("Entering GetStateRangeScanIteratorWithPagination() with namespace: %q, startKey: %q, endKey: %q, pageSize: %d", namespace, startKey, endKey, pageSize)
	internalQueryLimit := vdb.couchbaseInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		// loggerd.Debugf("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	scanner, err := newQueryScanner(namespace, db, "", internalQueryLimit, pageSize, "", startKey, endKey)
	if err != nil {
		// loggerd.Debugf("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting GetStateRangeScanIteratorWithPagination()")
	return scanner, nil
}

func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	// loggerd.Debugf("Entering ExecuteQuery() with namespace: %s, query: %s", namespace, query)
	internalQueryLimit := vdb.couchbaseInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		// loggerd.Debugf("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	scanner, err := newQueryScanner(namespace, db, query, internalQueryLimit, 0, "", "", "")
	if err != nil {
		// loggerd.Debugf("Exiting GetStateRangeScanIteratorWithPagination() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting ExecuteQuery()")
	return scanner, nil
}

func (vdb *VersionedDB) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (statedb.QueryResultsIterator, error) {
	// loggerd.Debugf("Entering ExecuteQueryWithPagination() with namespace: %s, query: %s, bookmark: %s, pageSize: %d", namespace, query, bookmark, pageSize)
	// loggerd.Debugf("Couchbase does not support")
	//return nil, nil

	internalQueryLimit := vdb.couchbaseInstance.internalQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		// loggerd.Debugf("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	//query, err = populateQuery(query, internalQueryLimit, bookmark, db)
	//if err != nil {
	////	 loggerd.Errorf("Error calling applyAdditionalQueryOptions(): %s", err.Error())
	////	 loggerd.Debugf()("Exiting ExecuteQueryWithPagination() with error: %s", err)
	//	return nil, err
	//}
	scanner, err := newQueryScanner(namespace, db, query, internalQueryLimit, pageSize, bookmark, "", "")
	if err != nil {
		// loggerd.Debugf("Exiting ExecuteQueryWithPagination() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting ExecuteQueryWithPagination()")
	return scanner, nil
}

func (vdb *VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	// loggerd.Debugf("Entering ApplyUpdates() with height: %v", height)

	if height != nil && batch.ContainsPostOrderWrites {
		// height is passed nil when committing missing private data for previously committed blocks
		r := &redoRecord{
			UpdateBatch: batch,
			Version:     height,
		}
		if err := vdb.redoLogger.persist(r); err != nil {
			// loggerd.Debugf("Exiting ApplyUpdates() with error persisting to redoLogger: %s", err)
			return err
		}
	}

	// stage 1 - buildCommitters builds committers per namespace (per DB). Each committer transforms the
	// given batch in the form of underlying db and keep it in memory.
	committers, err := vdb.buildCommitters(batch)
	if err != nil {
		// loggerd.Debugf("Exiting applyUpdates() with error building committers: %s", err)
		return err
	}

	if err = vdb.executeCommitter(committers); err != nil {
		// loggerd.Debugf("Exiting applyUpdates() with error executing committers: %s", err)
		return err
	}

	// Stgae 3 - postCommitProcessing - flush and record savepoint.
	namespaces := batch.GetUpdatedNamespaces()
	if err := vdb.postCommitProcessing(committers, namespaces, height); err != nil {
		// loggerd.Debugf("Exiting applyUpdates() with error in post commit processing: %s", err)
		return err
	}

	return nil
}

func (vdb *VersionedDB) postCommitProcessing(committers []*committer, namespaces []string, height *version.Height) error {
	// loggerd.Debugf("Entering postCommitProcessing() with %d committers, height: %v", len(committers), height)
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
		loggerp.Errorf("Error during recordSavepoint: %s", err.Error())
		// loggerd.Debugf("Exiting postCommitProcessing() with recordSavepoint error: %s", err)
		return err
	}

	wg.Wait()
	select {
	case err := <-errChan:
		// loggerd.Debugf("Exiting postCommitProcessing() with error from cache update: %s", err)
		return errors.WithStack(err)
	default:
		// loggerd.Debugf("Exiting postCommitProcessing() successfully")
		return nil
	}
}

func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
	// loggerd.Debugf("Entering GetLatestSavePoint()")
	var err error
	couchbaseDoc, err := vdb.metadataDB.readDoc(savepointDocID)
	if err != nil {
		loggerp.Errorf("Failed to read savepoint data %s", err.Error())
		if strings.Contains(err.Error(), "document not found") == true {
			// loggerd.Debugf("Exiting GetLatestSavePoint() with nil height, from error block")
			return nil, nil
		}
		// loggerd.Debugf("Exiting GetLatestSavePoint() with error: %s", err)
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return height nil
	if couchbaseDoc == nil {
		// loggerd.Debugf("Exiting GetLatestSavePoint() with nil height")
		return nil, nil
	}
	height, err := decodeSavepoint(couchbaseDoc)
	if err != nil {
		// loggerd.Debugf("Exiting GetLatestSavePoint() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting GetLatestSavePoint() with height: %v", height)
	return height, nil
}

func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	// loggerd.Debugf("Entering ValidateKeyValue() with key: %s", key)
	err := validateKey(key)
	if err != nil {
		// loggerd.Debugf("Exiting ValidateKeyValue() with error: %s", err)
		return err
	}
	err = validateValue(value)
	if err != nil {
		// loggerd.Debugf("Exiting ValidateKeyValue() with error: %s", err)
		return err
	}
	// loggerd.Debugf("Exiting ValidateKeyValue()")
	return nil
}

func (vdb *VersionedDB) BytesKeySupported() bool {
	// loggerd.Debugf("Entering BytesKeySupported()")
	// loggerd.Debugf("Exiting BytesKeySupported() with value: false")
	return false
}

func (vdb *VersionedDB) GetFullScanIterator(skipNamespace func(string) bool) (statedb.FullScanIterator, error) {
	// loggerd.Debugf("Entering GetFullScanIterator()")
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
			// loggerd.Debugf("Exiting GetFullScanIterator() with error: %s", err)
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
		// loggerd.Debugf("Exiting GetFullScanIterator() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting GetFullScanIterator()")
	return scanner, nil
}

func (vdb *VersionedDB) Open() error {
	// loggerd.Debugf("Entering Open()")
	//TODO implement me
	//panic("implement me")
	// loggerd.Debugf("Exiting Open()")
	return nil
}

func (vdb *VersionedDB) Close() {
	// loggerd.Debugf("Entering Close()")
	//TODO implement me, maybe close the couchbaseInstance inside ? Nope.
	//panic("implement me")
	// loggerd.Debugf("Exiting Close()")
}

// writeChannelMetadata saves channel metadata to metadataDB
func (vdb *VersionedDB) writeChannelMetadata() error {
	// loggerd.Debugf("Entering writeChannelMetadata()")
	vdb.channelMetadata.Id = channelMetadataDocID
	err := vdb.metadataDB.saveDoc(channelMetadataDocID, vdb.channelMetadata)
	if err != nil {
		// loggerd.Debugf("Exiting writeChannelMetadata() with error: %s", err)
		return err
	}
	// loggerd.Debugf("Exiting writeChannelMetadata()")
	return nil
}

// getNamespaceDBHandle gets the handle to a named chaincode database
func (vdb *VersionedDB) getNamespaceDBHandle(namespace string) (*couchbaseDatabase, error) {
	// loggerd.Debugf("Entering getNamespaceDBHandle() with namespace: %s", namespace)
	vdb.mux.RLock()
	db := vdb.namespaceDBs[namespace]
	vdb.mux.RUnlock()
	if db != nil {
		// loggerd.Debugf("Exiting getNamespaceDBHandle() with existing database handle")
		return db, nil
	}
	namespaceDBName := constructNamespaceDBName(vdb.chainName, namespace)
	vdb.mux.Lock()
	defer vdb.mux.Unlock()

	db = vdb.namespaceDBs[namespace]
	if db != nil {
		// loggerd.Debugf("Exiting getNamespaceDBHandle() with existing database handle")
		return db, nil
	}

	var err error
	if _, ok := vdb.channelMetadata.NamespaceDBsInfo[namespace]; !ok {
		loggerp.Debugf("[%s] add namespaceDBInfo for namespace %s", vdb.chainName, namespace)
		vdb.channelMetadata.NamespaceDBsInfo[namespace] = &namespaceDBInfo{
			Namespace: namespace,
			DBName:    namespaceDBName,
		}
		if err = vdb.writeChannelMetadata(); err != nil {
			// loggerd.Debugf("Exiting getNamespaceDBHandle() with error: %s", err)
			return nil, err
		}
	}
	db, err = createCouchbaseDatabase(vdb.couchbaseInstance, namespaceDBName)
	if err != nil {
		// loggerd.Debugf("Exiting getNamespaceDBHandle() with error: %s", err)
		return nil, err
	}
	vdb.namespaceDBs[namespace] = db
	// loggerd.Debugf("Exiting getNamespaceDBHandle() with new database handle")
	return db, nil
}

func (vdb *VersionedDB) readFromDB(namespace, key string) (*keyValue, error) {
	// loggerd.Debugf("Entering readFromDB() with namespace: %s, key: %s", namespace, key)
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		// loggerd.Debugf("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	if err := validateKey(key); err != nil {
		// loggerd.Debugf("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	couchbaseDoc, err := db.readDoc(key)
	if err != nil {
		// loggerd.Debugf("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	if couchbaseDoc == nil {
		// loggerd.Debugf("Exiting readFromDB() with nil document")
		return nil, nil
	}
	kv, err := couchbaseDocToKeyValue(couchbaseDoc)
	if err != nil {
		// loggerd.Debugf("Exiting readFromDB() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting readFromDB() with keyValue")
	return kv, nil
}

func (vdb *VersionedDB) GetState(namespace, key string) (*statedb.VersionedValue, error) {
	loggerp.Debugf("Entering GetState() with namespace=%s, key=%s", namespace, key)
	cacheEnabled := vdb.cache.enabled(namespace)
	if cacheEnabled {
		cv, err := vdb.cache.getState(vdb.chainName, namespace, key)
		if err != nil {
			// loggerd.Debugf("Exiting GetState() with error from cache: %s", err)
			return nil, err
		}
		if cv != nil {
			vv, err := constructVersionedValue(cv)
			if err != nil {
				// loggerd.Debugf("Exiting GetState() with error constructing value: %s", err)
				return nil, err
			}
			// loggerd.Debugf("Exiting GetState() with cached value, version: %v", vv.Version)
			return vv, nil
		}
	}
	kv, err := vdb.readFromDB(namespace, key)

	if kv == nil {
		// loggerd.Debugf("Exiting GetState() with nil value")
		return nil, nil
	}

	if err != nil {
		// loggerd.Debugf("Exiting GetState() with error: %s", err)
		return nil, err
	}

	if cacheEnabled {
		cacheValue := constructCacheValue(kv.VersionedValue)
		if err := vdb.cache.putState(vdb.chainName, namespace, key, cacheValue); err != nil {
			// loggerd.Debugf("Exiting GetState() with error storing in cache: %s", err)
			return nil, err
		}
	}
	// loggerd.Debugf("Exiting GetState()")
	//// loggerd.Debugf()("Exiting GetState() with value: %v", kv.VersionedValue)
	return kv.VersionedValue, nil
}

////////////// SUPPORT_FOR_INDEXES///////////////////////////////

// ProcessIndexesForChaincodeDeploy creates indexes for a specified namespace
func (vdb *VersionedDB) ProcessIndexesForChaincodeDeploy(namespace string, indexFilesData map[string][]byte) error {
	// loggerd.Debugf("Entering ProcessIndexesForChaincodeDeploy() with namespace: %s, %d index files", namespace, len(indexFilesData))
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		// loggerd.Debugf("Exiting ProcessIndexesForChaincodeDeploy() with error: %s", err)
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
			// loggerd.Errorf("error unmarshalling index data from file [%s] for chaincode [%s] on channel [%s]: %+v", fileName, namespace, vdb.chainName, err)
			continue
		}
		query := indexData.Index
		query, _, _ = populateQuery(query, 0, "", db)
		_, err := db.queryDocuments(query)
		if err != nil {
			loggerp.Errorf("error creating index from file [%s] for chaincode [%s] on channel [%s]: %+v", fileName, namespace, vdb.chainName, err)
			return err
		}
		loggerp.Infof("successfully submitted index creation request present in the file [%s] for chaincode [%s] on channel [%s]", fileName, namespace, vdb.chainName)
	}
	// loggerd.Debugf("Exiting ProcessIndexesForChaincodeDeploy() successfully")
	return nil
}

// GetDBType returns the hosted stateDB
func (vdb *VersionedDB) GetDBType() string {
	// loggerd.Debugf("Entering GetDBType()")
	// loggerd.Debugf("Exiting GetDBType() with value: couchbase")
	return "couchbase"
}

//////////////SUPPORT_FOR_BULK_OPTIMIZABLE///////////////////////

func (vdb *VersionedDB) LoadCommittedVersions(keys []*statedb.CompositeKey) error {
	// loggerd.Debugf("Entering LoadCommittedVersions() with %d keys", len(keys))
	missingKeys := make(map[string][]gocb.BulkOp)
	committedDataCache := newVersionCache()
	for _, compositeKey := range keys {
		ns, key := compositeKey.Namespace, compositeKey.Key
		committedDataCache.setVerAndRev(ns, key, nil)
		// loggerd.Debugf("Load into version cache: %s~%s", ns, key)

		if !vdb.cache.enabled(ns) {
			missingKeys[ns] = append(missingKeys[ns],
				&gocb.GetOp{
					ID: key,
				})
			continue
		}
		cv, err := vdb.cache.getState(vdb.chainName, ns, key)
		if err != nil {
			// loggerd.Debugf("Exiting LoadCommittedVersions() with error: %s", err)
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
			// loggerd.Debugf("Exiting LoadCommittedVersions() with error: %s", err)
			return err
		}
		committedDataCache.setVerAndRev(ns, key, vv.Version)
	}

	nsMetadataMap, err := vdb.retrieveMetadata(missingKeys)
	loggerp.Debugf("missingKeys=%s", missingKeys)
	loggerp.Debugf("nsMetadataMap=%v", nsMetadataMap)
	if err != nil {
		// loggerd.Debugf("Exiting LoadCommittedVersions() with error: %s", err)
		return err
	}
	for ns, nsMetadata := range nsMetadataMap {
		for _, keyMetadata := range nsMetadata {
			kv, err := couchbaseDocToKeyValue(keyMetadata)
			if err != nil {
				// loggerd.Debugf("Exiting LoadCommittedVersions() with error: %s", err)
				return err
			}
			committedDataCache.setVerAndRev(ns, kv.key, kv.Version)
		}
	}
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = committedDataCache
	// loggerd.Debugf("Exiting LoadCommittedVersions() successfully")
	return nil
}

// GetCachedVersion returns version from cache. `LoadCommittedVersions` function populates the cache
func (vdb *VersionedDB) GetCachedVersion(namespace string, key string) (*version.Height, bool) {
	// loggerd.Debugf("Entering GetCachedVersion() with namespace: %s, key: %s", namespace, key)
	loggerp.Debugf("Retrieving cached version: %s~%s", namespace, key)
	vdb.verCacheLock.RLock()
	defer vdb.verCacheLock.RUnlock()
	version, exists := vdb.committedDataCache.getVersion(namespace, key)
	// loggerd.Debugf("Exiting GetCachedVersion() with version: %v, exists: %t", version, exists)
	return version, exists
}

// ClearCachedVersions clears committedVersions and revisionNumbers
func (vdb *VersionedDB) ClearCachedVersions() {
	// loggerd.Debugf("Entering ClearCachedVersions()")
	loggerp.Debugf("Clear Cache")
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = newVersionCache()
	// loggerd.Debugf("Exiting ClearCachedVersions()")
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
	// loggerd.Debugf("Entering queryScanner.Next()")
	doc, err := scanner.next()
	if err != nil {
		// loggerd.Debugf("Exiting queryScanner.Next() with error: %s", err)
		return nil, err
	}
	if doc == nil {
		// loggerd.Debugf("Exiting queryScanner.Next() with nil document")
		return nil, nil
	}
	kv, err := couchbaseDocToKeyValue(doc)
	if err != nil {
		// loggerd.Debugf("Exiting queryScanner.Next() with error: %s", err)
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
	// loggerd.Debugf("Exiting queryScanner.Next() with key: %s", kv.key)
	return result, nil
}

func (scanner *queryScanner) getNextStateRangeScanResults() error {
	// loggerd.Debugf("Entering getNextStateRangeScanResults()")
	queryLimit := scanner.queryDefinition.internalQueryLimit
	if scanner.paginationInfo.requestedLimit > 0 {
		moreResultsNeeded := scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
		if moreResultsNeeded < scanner.queryDefinition.internalQueryLimit {
			queryLimit = moreResultsNeeded
		}
	}
	queryResult, nextStartKey, offset, err := scanner.db.readDocRange(scanner.queryDefinition.startKey, scanner.queryDefinition.endKey, queryLimit, scanner.offset)
	if err != nil {
		// loggerd.Debugf("Exiting getNextStateRangeScanResults() with error: %s", err)
		return err
	}
	// loggerd.Debugf("Size of queryResult: %d", len(queryResult))
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.cursor = 0
	if isEffectivelyEmpty(scanner.queryDefinition.startKey) && isEffectivelyEmpty(scanner.queryDefinition.endKey) {
		if offset == -1 {
			// loggerd.Debugf("Exiting getNextStateRangeScanResults() with offset (Exhausted): %d", offset)
			scanner.exhausted = true
		}
		scanner.offset = offset
	} else {
		if scanner.queryDefinition.endKey == nextStartKey {
			// as we always set inclusive_end=false to match the behavior of
			// goleveldb iterator, it is safe to mark the scanner as exhausted
			// loggerd.Debugf("Exiting getNextStateRangeScanResults() with endKey (Exhausted): %s", nextStartKey)
			scanner.exhausted = true
			// we still need to update the startKey as it is returned as bookmark
		}
		scanner.queryDefinition.startKey = nextStartKey
	}
	// loggerd.Debugf("Exiting getNextStateRangeScanResults()")
	return nil
}

func (scanner *queryScanner) next() (*couchbaseDoc, error) {
	// loggerd.Debugf("Entering queryScanner.next()")
	// loggerd.Debugf("QueryScanner.next() %v,", scanner.resultsInfo)
	// loggerd.Debugf("QueryScanner.next() cursor: %d, requestedLimit: %d, bookmark: %s,", scanner.paginationInfo.cursor, scanner.paginationInfo.requestedLimit, scanner.paginationInfo.bookmark)
	if len(scanner.resultsInfo.results) == 0 {
		// loggerd.Debugf("Exiting queryScanner.next() with nil document (no results)")
		return nil, nil
	}
	scanner.paginationInfo.cursor++
	if scanner.paginationInfo.cursor >= scanner.queryDefinition.internalQueryLimit {
		if scanner.exhausted {
			// loggerd.Debugf("Exiting queryScanner.next() with nil document (exhausted)")
			return nil, nil
		}
		var err error
		if scanner.queryDefinition.query != "" {
			err = scanner.executeQueryWithBookmark()
		} else {
			err = scanner.getNextStateRangeScanResults()
		}
		if err != nil {
			// loggerd.Debugf("Exiting queryScanner.next() with error: %s", err)
			return nil, err
		}
		if len(scanner.resultsInfo.results) == 0 {
			// loggerd.Debugf("Exiting queryScanner.next() with nil document (no new results)")
			return nil, nil
		}
	}
	if scanner.paginationInfo.cursor >= int32(len(scanner.resultsInfo.results)) {
		// loggerd.Debugf("Exiting queryScanner.next() with nil document (cursor beyond results)")
		return nil, nil
	}
	result := scanner.resultsInfo.results[scanner.paginationInfo.cursor]
	// loggerd.Debugf("Exiting queryScanner.next() with document: %v", result)
	return result, nil
}

func (scanner *queryScanner) Close() {
	// loggerd.Debugf("Entering queryScanner.Close()")
	// loggerd.Debugf("Exiting queryScanner.Close()")
}

func (scanner *queryScanner) GetBookmarkAndClose() string {
	// loggerd.Debugf("Entering queryScanner.GetBookmarkAndClose()")
	retval := ""
	if scanner.queryDefinition.query != "" {
		retval = scanner.paginationInfo.bookmark
	} else {
		retval = scanner.queryDefinition.startKey
	}
	scanner.Close()
	// loggerd.Debugf("Exiting queryScanner.GetBookmarkAndClose()")
	return retval
}

func newQueryScanner(namespace string, db *couchbaseDatabase, query string, internalQueryLimit,
	limit int32, bookmark, startKey, endKey string) (*queryScanner, error) {
	// loggerd.Debugf("Entering newQueryScanner() with namespace: %s, query: %s, limit: %d, bookmark: %s, startKey: %s, endKey: %s", namespace, query, limit, bookmark, startKey, endKey)
	scanner := &queryScanner{namespace, db, &queryDefinition{startKey, endKey, query, internalQueryLimit}, &paginationInfo{-1, limit, bookmark}, &resultsInfo{0, nil}, false, 0}
	var err error
	// query is defined, then execute the query and return the records and bookmark
	if scanner.queryDefinition.query != "" {
		err = scanner.executeQueryWithBookmark()
	} else {
		err = scanner.getNextStateRangeScanResults()
	}
	if err != nil {
		// loggerd.Debugf("Exiting newQueryScanner() with error: %s", err)
		return nil, err
	}
	scanner.paginationInfo.cursor = -1
	// loggerd.Debugf("Exiting newQueryScanner()")
	return scanner, nil
}

// executeQueryWithBookmark executes a "paging" query with a bookmark, this method allows a
// paged query without returning a new query iterator
func (scanner *queryScanner) executeQueryWithBookmark() error {
	// loggerd.Debugf("Entering executeQueryWithBookmark()")
	queryLimit := scanner.queryDefinition.internalQueryLimit
	if scanner.paginationInfo.requestedLimit > 0 {
		if scanner.paginationInfo.requestedLimit-scanner.resultsInfo.totalRecordsReturned < scanner.queryDefinition.internalQueryLimit {
			queryLimit = scanner.paginationInfo.requestedLimit - scanner.resultsInfo.totalRecordsReturned
		}
	}
	queryString, updatedBookmark, err := populateQuery(scanner.queryDefinition.query,
		queryLimit, scanner.paginationInfo.bookmark, scanner.db)
	if err != nil {
		// loggerd.Debugf("Error calling applyAdditionalQueryOptions(): %s\n", err.Error())
		// loggerd.Debugf("Exiting executeQueryWithBookmark() with error: %s", err)
		return err
	}
	queryResult, err := scanner.db.queryDocuments(queryString)
	if err != nil {
		// loggerd.Debugf("Error calling QueryDocuments(): %s\n", err.Error())
		// loggerd.Debugf("Exiting executeQueryWithBookmark() with error: %s", err)
		return err
	}
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.bookmark = updatedBookmark
	scanner.paginationInfo.cursor = 0
	// loggerd.Debugf("Exiting executeQueryWithBookmark() with bookmark: %s, queryResult: %v", updatedBookmark, queryResult)
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
	// loggerd.Debugf("Entering newDBsScanner() with %d databases to scan", len(dbsToScan))
	if len(dbsToScan) == 0 {
		// loggerd.Debugf("Exiting newDBsScanner() with nil scanner (no databases)")
		return nil, nil
	}
	s := &dbsScanner{
		dbs:                   dbsToScan,
		prefetchLimit:         prefetchLimit,
		toSkipKeysFromEmptyNs: toSkipKeysFromEmptyNs,
	}
	if err := s.beginNextDBScan(); err != nil {
		// loggerd.Debugf("Exiting newDBsScanner() with error: %s", err)
		return nil, err
	}
	// loggerd.Debugf("Exiting newDBsScanner()")
	return s, nil
}

func (s *dbsScanner) beginNextDBScan() error {
	// loggerd.Debugf("Entering beginNextDBScan()")
	dbUnderScan := s.dbs[s.nextDBToScanIndex]
	queryScanner, err := newQueryScanner(dbUnderScan.ns, dbUnderScan.db, "", s.prefetchLimit, 0, "", "", "")
	if err != nil {
		// loggerd.Debugf("Exiting beginNextDBScan() with error: %s", err)
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
	// loggerd.Debugf("Exiting beginNextDBScan()")
	return nil
}

// Next returns the key-values present in the namespaceDB. Once a namespaceDB
// is processed, it moves to the next namespaceDB till all are processed.
func (s *dbsScanner) Next() (*statedb.VersionedKV, error) {
	// loggerd.Debugf("Entering dbsScanner.Next()")
	if s == nil {
		// loggerd.Debugf("Exiting dbsScanner.Next() with nil value (scanner is nil)")
		return nil, nil
	}
	for {
		couchbaseDoc, err := s.resultItr.next()
		// loggerd.Debugf("Iterating DbScanner Next() %v,", couchbaseDoc)
		if err != nil {
			// loggerd.Debugf("Exiting dbsScanner.Next() with error: %s", err)
			return nil, errors.WithMessagef(
				err,
				"failed to retrieve the next entry from scanner associated with namespace %s",
				s.currentNamespace,
			)
		}
		if couchbaseDoc == nil {
			s.resultItr.Close()
			if len(s.dbs) <= s.nextDBToScanIndex {
				// loggerd.Debugf("Exiting dbsScanner.Next() with nil value (all databases processed)")
				break
			}
			if err := s.beginNextDBScan(); err != nil {
				// loggerd.Debugf("Exiting dbsScanner.Next() with error: %s", err)
				return nil, err
			}
			continue
		}
		if s.currentNamespace == "" {
			key, err := couchbaseDoc.key()
			if err != nil {
				// loggerd.Debugf("Exiting dbsScanner.Next() with error: %s", err)
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
			// loggerd.Debugf("Exiting dbsScanner.Next() with error: %s", err)
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
		// loggerd.Debugf("Exiting dbsScanner.Next() with key: %s", kv.key)
		return result, nil
	}
	// loggerd.Debugf("Exiting dbsScanner.Next() with nil value")
	return nil, nil
}

func (s *dbsScanner) Close() {
	// loggerd.Debugf("Entering dbsScanner.Close()")
	if s == nil {
		// loggerd.Debugf("Exiting dbsScanner.Close() (scanner is nil)")
		return
	}
	s.resultItr.Close()
	// loggerd.Debugf("Exiting dbsScanner.Close()")
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
	// loggerd.Debugf("Entering importState()")
	if s.itr == nil {
		// loggerd.Debugf("Exiting importState() (iterator is nil)")
		return nil
	}
	for {
		versionedKV, err := s.itr.Next()
		if err != nil {
			// loggerd.Debugf("Exiting importState() with error: %s", err)
			return err
		}
		if versionedKV == nil {
			break
		}

		switch {
		case s.currentNsDB == nil:
			if err := s.createDBForNamespace(versionedKV.Namespace); err != nil {
				// loggerd.Debugf("Exiting importState() with error: %s", err)
				return err
			}
		case s.currentNs != versionedKV.Namespace:
			if err := s.storePendingDocs(); err != nil {
				// loggerd.Debugf("Exiting importState() with error: %s", err)
				return err
			}
			if err := s.createDBForNamespace(versionedKV.Namespace); err != nil {
				// loggerd.Debugf("Exiting importState() with error: %s", err)
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
			// loggerd.Debugf("Exiting importState() with error: %s", err)
			return err
		}
		s.pendingDocsBatch = append(s.pendingDocsBatch, doc)
		s.batchMemorySize += len(*doc)

		// TODO populate with max batch update size.
		if s.batchMemorySize >= maxDataImportBatchMemorySize ||
			len(s.pendingDocsBatch) == 100 {
			if err := s.storePendingDocs(); err != nil {
				// loggerd.Debugf("Exiting importState() with error: %s", err)
				return err
			}
		}
	}

	err := s.storePendingDocs()
	if err != nil {
		// loggerd.Debugf("Exiting importState() with error: %s", err)
		return err
	}
	// loggerd.Debugf("Exiting importState()")
	return nil
}

func (s *snapshotImporter) createDBForNamespace(ns string) error {
	// loggerd.Debugf("Entering createDBForNamespace() with namespace: %s", ns)
	s.currentNs = ns
	var err error
	s.currentNsDB, err = s.vdb.getNamespaceDBHandle(ns)
	if err != nil {
		// loggerd.Debugf("Exiting createDBForNamespace() with error: %s", err)
		return errors.WithMessagef(err, "error while creating database for the namespace %s", ns)
	}
	// loggerd.Debugf("Exiting createDBForNamespace()")
	return nil
}

func (s *snapshotImporter) storePendingDocs() error {
	// loggerd.Debugf("Entering storePendingDocs() with %d pending documents", len(s.pendingDocsBatch))
	if len(s.pendingDocsBatch) == 0 {
		// loggerd.Debugf("Exiting storePendingDocs() (no pending docs)")
		return nil
	}

	if err := s.currentNsDB.insertDocuments(s.pendingDocsBatch); err != nil {
		// loggerd.Debugf("Exiting storePendingDocs() with error: %s", err)
		return errors.WithMessagef(
			err,
			"error while storing %d states associated with namespace %s",
			len(s.pendingDocsBatch), s.currentNs,
		)
	}
	s.batchMemorySize = 0
	s.pendingDocsBatch = nil

	// loggerd.Debugf("Exiting storePendingDocs()")
	return nil
}
