package statecouchbase

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
	"log"
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

type couchbaseDoc struct {
	jsonValue  []byte
	attachment *CouchbaseAttachment
}

type CouchbaseAttachment struct {
	Attachment []byte `json:"_attachment"`
}

type couchbaseDatabase struct {
	couchbaseInstance *couchbaseInstance // connection configuration
	dbName            string             // dbName is the name of the Collection(wrt couchbase)
}

// queryResult is used for returning query results from CouchDB
type queryResult struct {
	id    string
	value []byte
}

type dbInfo struct {
	DbName string `json:"db_name"`
	Sizes  struct {
		File     int `json:"file"`
		External int `json:"external"`
		Active   int `json:"active"`
	} `json:"sizes"`
	Other struct {
		DataSize int `json:"data_size"`
	} `json:"other"`
	DocDelCount       int    `json:"doc_del_count"`
	DocCount          int    `json:"doc_count"`
	DiskSize          int    `json:"disk_size"`
	DiskFormatVersion int    `json:"disk_format_version"`
	DataSize          int    `json:"data_size"`
	CompactRunning    bool   `json:"compact_running"`
	InstanceStartTime string `json:"instance_start_time"`
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

	couchbaseDoc.attachment = &attachment
	//jsonBytes, err := jsonValue.MarshalJSON()
	//if err != nil {
	//	return nil, err
	//}

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
	couchbaseLogger.Infof("[%s] Entering queryDocuments()", dbclient.dbName)
	var results []*queryResult

	rows, err := dbclient.couchbaseInstance.cluster.Query(query, nil)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var row json.RawMessage
		var result = &queryResult{}
		var docMetadata docMetadata
		err := rows.Row(&row)
		result.value = row
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(row, &docMetadata)
		if err != nil {
			return nil, err
		}
		result.id = docMetadata.ID
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

func (dbclient *couchbaseDatabase) readDocRange(startKey, endKey string, limit int32) ([]*queryResult, string, error) {
	dbName := dbclient.dbName
	couchbaseLogger.Infof("[%s] Entering ReadDocRange()  startKey=%s, endKey=%s", dbName, startKey, endKey)

	var results []*queryResult
	limitUnsigned := uint32(limit)
	scan, err := dbclient.couchbaseInstance.scope.Collection(dbName).Scan(gocb.RangeScan{
		From: &gocb.ScanTerm{Term: startKey, Exclusive: false},
		To:   &gocb.ScanTerm{Term: endKey, Exclusive: true},
	}, &gocb.ScanOptions{BatchItemLimit: &limitUnsigned})
	if err != nil {
		return nil, "", err
	}

	for {
		doc := scan.Next()
		var docMetadata docMetadata
		if doc != nil {
			break
		}
		var row json.RawMessage
		err := doc.Content(&row)
		if err != nil {
			return nil, "", err
		}
		err = json.Unmarshal(row, &docMetadata)
		if err != nil {
			return nil, "", err
		}
		results = append(results, &queryResult{id: docMetadata.ID, value: row})
	}

	couchbaseLogger.Infof("[%s] Exiting ReadDocRange()", dbclient.dbName)

	return results, "", nil
}
