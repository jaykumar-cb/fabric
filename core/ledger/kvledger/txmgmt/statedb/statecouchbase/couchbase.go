package statecouchbase

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
	"log"
	"strings"
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

type couchbaseDoc struct {
	jsonValue  []byte
	attachment []byte
}

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

type queryResult map[string]interface{}

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
	err := dbclient.couchbaseInstance.bucket.CollectionsV2().CreateCollection(dbclient.couchbaseInstance.conf.Scope, dbclient.dbName, nil, nil)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error creating collection: %s", dbclient.dbName, err)
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
	var couchbaseDoc couchbaseDoc
	document, err := dbclient.couchbaseInstance.scope.Collection(dbclient.dbName).Get(key, nil)
	if err != nil {
		couchbaseLogger.Errorf("[%s] Error reading key: %s, Error: %s", dbclient.dbName, key, err)
		return nil, err
	}

	var jsonValue json.RawMessage
	var attachment CouchbaseAttachment

	err = document.Content(&jsonValue)
	if err != nil {
		return nil, err
	}

	err = document.Content(&attachment)
	if err != nil {
		return nil, err
	}

	couchbaseDoc.attachment = attachment.Attachment

	couchbaseLogger.Infof("[%s] readDoc() for key=%s, value=%s", dbclient.dbName, key, string(jsonValue))

	couchbaseDoc.jsonValue = jsonValue
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

func (dbclient *couchbaseDatabase) queryDocuments(query string) ([]*queryResult, error) {
	couchbaseLogger.Infof("[%s] Entering queryDocuments() with query: %s", dbclient.dbName, query)
	var results []*queryResult

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
		var result = &queryResult{}
		err := rows.Row(&result)
		rows.Raw()
		if err != nil {
			return nil, err
		}
		results = append(results, result)
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
	responses, err := dbclient.batchUpdateDocuments(docs)
	if err != nil {
		return errors.WithMessage(err, "error while updating docs in bulk")
	}

	for i, resp := range responses {
		if resp.Ok {
			continue
		}
		if err := dbclient.saveDoc(resp.ID, docs[i]); err != nil {
			return errors.WithMessagef(err, "error while storing doc with ID %s", resp.ID)
		}
	}
	couchbaseLogger.Infof("[%s] Exiting insertDocuments()", dbclient.dbName)
	return nil
}

type batchUpdateResponse struct {
	ID string
	Ok bool
}

// batchUpdateDocuments - batch method to batch update documents
func (dbclient *couchbaseDatabase) batchUpdateDocuments(documents []*couchbaseDoc) ([]*batchUpdateResponse, error) {
	dbName := dbclient.dbName
	couchbaseLogger.Infof("[%s] Entering batchUpdateDocuments()", dbName)
	var response []*batchUpdateResponse
	// TODO use configuration file for this Refer: https://docs.couchbase.com/go-sdk/current/howtos/concurrent-async-apis.html#sizing-batches-examples
	batches, err := buildBatches(documents, 10)
	if err != nil {
		return nil, err
	}
	for _, batch := range batches {
		err := dbclient.couchbaseInstance.scope.Collection(dbName).Do(batch, nil)
		if err != nil {
			log.Println(err)
		}

		// Be sure to check each individual operation for errors too.
		for _, op := range batch {
			upsertOp := op.(*gocb.UpsertOp)
			// TODO check the id here is it the document ID ? This is possibly a bug.
			response = append(response, &batchUpdateResponse{
				ID: upsertOp.ID,
				Ok: upsertOp.Err == nil,
			})
			if upsertOp.Err != nil {
				couchbaseLogger.Infof("Error upserting document with ID %s: %v", upsertOp.ID, upsertOp.Err)
			}
		}
	}
	if couchbaseLogger.IsEnabledFor(zapcore.DebugLevel) {
		documentIdsString, err := printDocumentIds(documents)
		if err == nil {
			couchbaseLogger.Infof("[%s] Entering BatchUpdateDocuments()  document ids=[%s]", dbName, documentIdsString)
		} else {
			couchbaseLogger.Infof("[%s] Entering BatchUpdateDocuments()  Could not print document ids due to error: %+v", dbName, err)
		}
	}
	couchbaseLogger.Infof("[%s] Exiting batchUpdateDocuments()", dbName)
	return response, nil
}

func buildBatches(documents []*couchbaseDoc, numBatches int) (map[int][]gocb.BulkOp, error) {
	couchbaseLogger.Infof("Entering buildBatches()")
	batches := make(map[int][]gocb.BulkOp)
	for i, document := range documents {
		var docContent interface{}
		var docMetadata docMetadata
		err := json.Unmarshal(document.jsonValue, &docContent)
		err = json.Unmarshal(document.jsonValue, &docMetadata)
		if err != nil {
			return nil, err
		}
		_, ok := batches[i%numBatches]
		if !ok {
			batches[i%numBatches] = []gocb.BulkOp{}
		}
		batches[i%numBatches] = append(batches[i%numBatches], &gocb.UpsertOp{
			ID:    docMetadata.ID,
			Value: docContent,
		})
	}
	couchbaseLogger.Infof("Exiting buildBatches()")
	return batches, nil
}

func isEffectivelyEmpty(s string) bool {
	return strings.TrimSpace(s) == "" || s == "\x00" || s == "\x01"
}

func (dbclient *couchbaseDatabase) readDocRange(startKey, endKey string, limit int32, offset int32) ([]*queryResult, string, int32, error) {
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
