package statecouchbase

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
)

var couchbaseLogger = flogging.MustGetLogger("couchbase")

const retryWaitTime = 125

//const CapellaApiUrl = "https://cloudapi.cloud.couchbase.com/v4"

type docMetadata struct {
	ID      string `json:"_id"`
	Version string `json:"~version"`
}

type couchbaseInstance struct {
	conf    *ledger.CouchbaseConfig
	cluster *gocb.Cluster
	bucket  *gocb.Bucket
	scope   *gocb.Scope
}

func (couchbaseInstance *couchbaseInstance) internalQueryLimit() int32 {
	return 1000
}

type couchbaseDoc map[string]interface{}

type CouchbaseAttachment struct {
	Attachment []byte `json:"_attachment"`
}

type couchbaseDatabase struct {
	couchbaseInstance *couchbaseInstance // connection configuration
	dbName            string             // dbName is the name of the Collection(wrt couchbase)
}

// queryResult is used for returning query results from CouchDB
//type queryResult struct {
//	id         string
//	value      []byte
//	attachment []byte
//}

type IndexData struct {
	Index string `json:"index"`
}

//func unMarshallSdkResponse(result *gocb.QueryResult) ([]bytes, error) {
//
//}

func getAllDatabases(couchbaseInstance *couchbaseInstance) []string {
	couchbaseLogger.Infof("Entering getAllDatabases()")
	var allCollections []string
	scopes, err := couchbaseInstance.bucket.CollectionsV2().GetAllScopes(nil)
	if err != nil {
		return nil
	}
	for _, scope := range scopes {
		for _, collection := range scope.Collections {
			if strings.HasPrefix(collection.Name, "_") {
				continue
			}
			allCollections = append(allCollections, collection.Name)
		}
	}
	couchbaseLogger.Infof("Exiting getAllDatabases()")
	return allCollections
}

func (dbclient *couchbaseDatabase) checkDatabaseExists() bool {
	couchbaseLogger.Infof("[%s] Entering checkDatabaseExists()", dbclient.dbName)
	allDatabases := getAllDatabases(dbclient.couchbaseInstance)
	for _, collection := range allDatabases {
		if collection == dbclient.dbName {
			couchbaseLogger.Infof("[%s] Exiting checkDatabaseExists() - found", dbclient.dbName)
			return true
		}
	}
	couchbaseLogger.Infof("[%s] Exiting checkDatabaseExists() - not found", dbclient.dbName)
	return false
}

func (dbclient *couchbaseDatabase) createDatabase() error {
	couchbaseLogger.Infof("[%s] Entering CreateDatabase()", dbclient.dbName)

	// Create the collection
	err := dbclient.couchbaseInstance.bucket.CollectionsV2().CreateCollection(dbclient.couchbaseInstance.conf.Scope, dbclient.dbName, nil, &gocb.CreateCollectionOptions{
		Timeout: 20 * time.Second,
	})

	if err != nil {
		couchbaseLogger.Errorf("[%s] Error creating collection: %s", dbclient.dbName, err)
		return err
	}

	time.Sleep(10 * time.Second)

	//IndexCreationQuery, _, _ := populateQuery("CREATE PRIMARY INDEX ON {{ .Source }}", 0, "", dbclient)
	//couchbaseLogger.Infof("Index Query: %s", IndexCreationQuery)
	//_, err = dbclient.queryDocuments(IndexCreationQuery)
	//if err != nil {
	//	couchbaseLogger.Errorf("[%s] Error creating index: %s", dbclient.dbName, err)
	//	return err
	//}

	err = dbclient.couchbaseInstance.cluster.QueryIndexes().CreatePrimaryIndex(
		dbclient.couchbaseInstance.conf.Bucket,
		&gocb.CreatePrimaryQueryIndexOptions{
			Timeout:        20 * time.Second,
			IgnoreIfExists: true,
			ScopeName:      dbclient.couchbaseInstance.conf.Scope,
			CollectionName: dbclient.dbName,
		},
	)

	if err != nil {
		fmt.Printf("Error creating primary index: %s", err)
		return err
	}

	couchbaseLogger.Infof("[%s] Exiting CreateDatabase()", dbclient.dbName)
	return nil
}

// createDatabaseIfNotExist method provides function to create database
func (dbclient *couchbaseDatabase) createDatabaseIfNotExist() error {
	couchbaseLogger.Infof("[%s] Entering CreateDatabaseIfNotExist()", dbclient.dbName)

	collectionExists := dbclient.checkDatabaseExists()

	if collectionExists == false {
		couchbaseLogger.Infof("[%s] CreateDatabaseIfNotExist() - collection does not exist, creating a new one", dbclient.dbName)
		err := dbclient.createDatabase()
		if err != nil {
			return err
		}
	}
	couchbaseLogger.Infof("Created state database %s", dbclient.dbName)
	couchbaseLogger.Infof("[%s] Exiting CreateDatabaseIfNotExist()", dbclient.dbName)
	return nil
}

func (dbclient *couchbaseDatabase) readDoc(key string) (*couchbaseDoc, error) {
	couchbaseLogger.Infof("[%s] Entering readDoc() for key=%s", dbclient.dbName, key)
	couchbaseDoc := make(couchbaseDoc)
	document, err := dbclient.couchbaseInstance.scope.Collection(dbclient.dbName).Get(key, nil)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error reading key: %s, Error: %s", dbclient.dbName, key, err)
		return nil, err
	}

	err = document.Content(&couchbaseDoc)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error reading key: %s, Error: %s", dbclient.dbName, key, err)
		return nil, err
	}
	//couchbaseLogger.Infof("[%s] readDoc() for key=%s, value=%s", dbclient.dbName, key, couchbaseDoc)
	couchbaseLogger.Infof("[%s] Exiting readDoc() for key=%s", dbclient.dbName, key)
	return &couchbaseDoc, nil
}

func (dbclient *couchbaseDatabase) saveDoc(key string, value interface{}) error {
	couchbaseLogger.Infof("[%s] Entering saveDoc() for key=%s", dbclient.dbName, key)
	_, err := dbclient.couchbaseInstance.scope.Collection(dbclient.dbName).Upsert(key, value, nil)
	if err != nil {
		return err
	}
	couchbaseLogger.Infof("[%s] Exiting saveDoc() for key=%s", dbclient.dbName, key)
	return nil
}

func (dbclient *couchbaseDatabase) queryDocuments(query string) ([]*couchbaseDoc, error) {
	couchbaseLogger.Infof("[%s] Entering queryDocuments() with query: %s", dbclient.dbName, query)
	results := make([]*couchbaseDoc, 0)

	rows, err := dbclient.couchbaseInstance.cluster.Query(query, nil)
	if err != nil {
		return nil, err
	}
	//if !strings.Contains(query, "INDEX") {
	//	for rows.Next() {
	//		row := make(jsonValue)
	//		var result = &queryResult{}
	//		var attachment CouchbaseAttachment
	//
	//		err := rows.Row(&row)
	//
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		err = rows.Row(&attachment)
	//
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		result.id = row[idField].(string)
	//
	//		if attachment.Attachment != nil {
	//			result.attachment = attachment.Attachment
	//		}
	//
	//		rowBytes, err := row.toBytes()
	//
	//		result.value = rowBytes
	//		couchbaseLogger.Infof("Processed document: %s", result.id)
	//		results = append(results, result)
	//	}
	//}

	for rows.Next() {
		result := make(couchbaseDoc)
		err := rows.Row(&result)
		if err != nil {
			return nil, err
		}
		results = append(results, &result)
	}
	couchbaseLogger.Infof("[%s] Exiting queryDocuments()", dbclient.dbName)
	return results, nil
}

// dropDatabase provides method to drop an existing database
func (dbclient *couchbaseDatabase) dropDatabase() error {
	dbName := dbclient.dbName

	couchbaseLogger.Infof("[%s] Entering DropDatabase()", dbName)

	err := dbclient.couchbaseInstance.bucket.CollectionsV2().DropCollection(dbclient.couchbaseInstance.conf.Scope, dbName, nil)

	if err != nil {
		return err
	}

	couchbaseLogger.Infof("[%s] Exiting DropDatabase(), database dropped", dbclient.dbName)
	return nil
}

func (dbclient *couchbaseDatabase) insertDocuments(docs []*couchbaseDoc) error {
	couchbaseLogger.Infof("[%s] Entering insertDocuments()", dbclient.dbName)
	batch := make([]gocb.BulkOp, len(docs))
	for _, doc := range docs {
		batch = append(batch, &gocb.UpsertOp{
			ID:    (*doc)[idField].(string),
			Value: doc,
		})
	}
	err := dbclient.batchUpdateDocuments(batch)
	if err != nil {
		return errors.WithMessage(err, "error while updating docs in bulk")
	}
	return nil
}

type batchUpdateResponse struct {
	ID string
	Ok bool
}

func (dbclient *couchbaseDatabase) deleteDocument(key string) error {
	couchbaseLogger.Infof("[%s] Entering deleteDocument() for key=%s", dbclient.dbName, key)
	_, err := dbclient.couchbaseInstance.scope.Collection(dbclient.dbName).Remove(key, nil)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error deleting key: %s, Error: %s", dbclient.dbName, key, err)
		return err
	}
	couchbaseLogger.Infof("[%s] Exiting deleteDocument() for key=%s", dbclient.dbName, key)
	return nil
}

// batchUpdateDocuments - batch method to batch update documents
func (dbclient *couchbaseDatabase) batchUpdateDocuments(batch []gocb.BulkOp) error {
	dbName := dbclient.dbName
	couchbaseLogger.Infof("[%s] Entering batchUpdateDocuments()", dbName)

	var errsChan = make(chan error, 1000)
	defer close(errsChan)

	err := dbclient.couchbaseInstance.scope.Collection(dbName).Do(batch, nil)
	if err != nil {
		log.Println(err)
		return err
	}

	// Check each individual operation for errors too.
	var checkWg sync.WaitGroup
	checkWg.Add(len(batch))
	for _, op := range batch {
		go func(batch []gocb.BulkOp) {
			checkWg.Done()
			switch opTyped := op.(type) {
			case *gocb.UpsertOp:
				if opTyped.Err != nil {
					couchbaseLogger.Errorf("[%s] Error upserting document: %s, Retrying... %+v", dbclient.dbName, opTyped.Err, opTyped)
					if err := dbclient.saveDoc(opTyped.ID, opTyped.Value); err != nil {
						errsChan <- errors.WithMessagef(err, "error while storing doc with ID %s", opTyped.ID)
					}
				}
			case *gocb.RemoveOp:
				if opTyped.Err != nil {
					couchbaseLogger.Errorf("[%s] Error removing document: %s, Retrying... %+v", dbclient.dbName, opTyped.Err, opTyped)
					//if err := dbclient.removeDoc(opTyped.ID); err != nil {
					//	errsChan <- errors.WithMessagef(err, "error while removing doc with ID %s", opTyped.ID)
					//}
				}
			default:
				couchbaseLogger.Warnf("Unknown operation type: %+v", op)
			}
		}(batch)
	}
	checkWg.Wait()

	select {
	case err := <-errsChan:
		couchbaseLogger.Infof("[%s] Exiting batchUpdateDocuments() with error", dbclient.dbName)
		return errors.WithStack(err)
	default:
		couchbaseLogger.Infof("[%s] Exiting batchUpdateDocuments()", dbName)
		return nil
	}
}

func (dbclient *couchbaseDatabase) batchGetDocument(keys []gocb.BulkOp) ([]*couchbaseDoc, error) {
	dbName := dbclient.dbName
	couchbaseLogger.Infof("[%s] Entering batchGetDocument()", dbName)

	results := make([]*couchbaseDoc, 0, len(keys))

	err := dbclient.couchbaseInstance.scope.Collection(dbName).Do(keys, nil)
	if err != nil {
		log.Println(err)
	}
	for _, op := range keys {
		response := make(couchbaseDoc)

		getOp := op.(*gocb.GetOp)

		if getOp.Err != nil {
			if strings.Contains(getOp.Err.Error(), "document not found") {
				couchbaseLogger.Infof("Document with ID %s not found", getOp.ID)
				continue
			} else {
				couchbaseLogger.Infof("Error getting document with ID %s: %v", getOp.ID, getOp.Err)
				return nil, getOp.Err
			}
		}

		err := getOp.Result.Content(&response)

		results = append(results, &response)

		if err != nil {
			couchbaseLogger.Infof("Error getting document with ID %s: %v", getOp.ID, err)
			return nil, err
		}
	}
	return results, err
}

func buildGetBatches(keys []string, documentsPerBatch int) ([][]gocb.BulkOp, error) {
	couchbaseLogger.Infof("Entering buildGetBatches()")

	var batches [][]gocb.BulkOp
	var currentBatch []gocb.BulkOp

	for i, key := range keys {
		op := &gocb.GetOp{
			ID: key,
		}
		currentBatch = append(currentBatch, op)

		// If batch is full or it's the last document, finalize the batch
		if len(currentBatch) == documentsPerBatch || i == len(keys)-1 {
			batches = append(batches, currentBatch)
			currentBatch = nil
		}
	}

	couchbaseLogger.Infof("Exiting buildGetBatches() with %d batches", len(batches))
	return batches, nil
}

func buildUpdateBatches(documents []*couchbaseDoc, documentsPerBatch int) ([][]gocb.BulkOp, error) {
	couchbaseLogger.Infof("Entering buildUpdateBatches()")

	var batches [][]gocb.BulkOp
	var currentBatch []gocb.BulkOp

	for i, document := range documents {
		op := &gocb.UpsertOp{
			ID:    (*document)[idField].(string),
			Value: document,
		}
		currentBatch = append(currentBatch, op)

		// If batch is full or it's the last document, finalize the batch
		if len(currentBatch) == documentsPerBatch || i == len(documents)-1 {
			batches = append(batches, currentBatch)
			currentBatch = nil
		}
	}

	couchbaseLogger.Infof("Exiting buildUpdateBatches() with %d batches", len(batches))
	return batches, nil
}

func isEffectivelyEmpty(s string) bool {
	return strings.TrimSpace(s) == "" || s == "\x00" || s == "\x01"
}

func (dbclient *couchbaseDatabase) readDocRange(startKey, endKey string, limit int32, offset int32) ([]*couchbaseDoc, string, int32, error) {
	dbName := dbclient.dbName
	newOffset := int32(-1)
	//limit += 1
	couchbaseLogger.Infof("[%s] Entering readDocRange()  startKey=[%q], endKey=[%q] limit=[%d]", dbName, startKey, endKey, limit)
	var query string
	nextStartKey := ""
	//if limit > 0 {
	if isEffectivelyEmpty(startKey) && isEffectivelyEmpty(endKey) {
		couchbaseLogger.Infof("[%s] readDocRange() - no startKey and endKey provided, using limit", dbclient.dbName)
		query = fmt.Sprintf("SELECT a.* FROM `%s`.`%s`.`%s` as a ORDER BY META().id ASC LIMIT %d OFFSET %d", dbclient.couchbaseInstance.conf.Bucket, dbclient.couchbaseInstance.conf.Scope, dbclient.dbName, limit+1, offset)
	} else {
		query = fmt.Sprintf("SELECT a.* FROM `%s`.`%s`.`%s` as a WHERE META().id >= '%s' AND META().id <= '%s' ORDER BY META().id ASC LIMIT %d", dbclient.couchbaseInstance.conf.Bucket, dbclient.couchbaseInstance.conf.Scope, dbclient.dbName, startKey, endKey, limit+1)
	}
	//}

	results, err := dbclient.queryDocuments(query)
	if err != nil {
		return nil, "", 0, err
	}

	if isEffectivelyEmpty(startKey) && isEffectivelyEmpty(endKey) {
		couchbaseLogger.Infof("len(results) = %d, limit = %d", len(results), int(limit)+1)
		if len(results) == int(limit)+1 {
			couchbaseLogger.Infof("len(results) = %d, limit = %d YES", len(results), int(limit)+1)
			newOffset = offset + limit
			results = results[:len(results)-1]
		}
	} else {
		if len(results) != 0 {
			nextStartKey = (*results[len(results)-1])[idField].(string)
			results = results[:len(results)-1]
		}
	}

	couchbaseLogger.Infof("[%s] Exiting readDocRange()  startKey=[%q], endKey=[%q] results=[%v], nextStartKey=[%s] offset=[%d]", dbclient.dbName, startKey, endKey, results, nextStartKey, newOffset)

	return results, nextStartKey, newOffset, nil
}
