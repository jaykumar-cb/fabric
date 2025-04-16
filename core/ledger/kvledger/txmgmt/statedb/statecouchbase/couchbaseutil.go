package statecouchbase

import (
	"encoding/hex"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"time"
)

// https://docs.couchbase.com/server/current/learn/data/scopes-and-collections.html#naming-for-scopes-and-collections
var (
	expectedDatabaseNamePattern = `[A-Za-z0-9-][A-Za-z0-9_%\-]*`
	maxLength                   = 251
)

var (
	chainNameAllowedLength      = 50
	namespaceNameAllowedLength  = 50
	collectionNameAllowedLength = 50
	disableKeepAlive            bool
)

func createCouchbaseInstance(config *ledger.CouchbaseConfig) (*couchbaseInstance, error) {
	fmt.Printf("Creating couchbase instance with config %+v\n", config)
	address := config.Address

	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		},
	}

	if err := options.ApplyProfile(gocb.ClusterConfigProfileWanDevelopment); err != nil {
		return nil, err
	}

	client, err := gocb.Connect(address, options)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(config.Bucket)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		return nil, err
	}

	scope := bucket.Scope(config.Scope)

	// Create the Couchbase instance
	couchbaseInstance := &couchbaseInstance{
		conf:    config,
		cluster: client,
		bucket:  bucket,
		scope:   scope,
	}

	return couchbaseInstance, nil
}

// constructMetadataDBName truncates the db name to couchdb allowed length to
// construct the metadataDBName
// Note:
// Currently there is a non-deterministic collision between metadataDB and namespaceDB with namespace="".
// When channel name is not truncated, metadataDB and namespaceDB with namespace="" have the same db name.
// When channel name is truncated, these two DBs have different db names.
// We have to deal with this behavior for now. In the future, we may rename metadataDB and
// migrate the content to avoid the collision.
func constructMetadataDBName(dbName string) string {
	if len(dbName) > maxLength {
		untruncatedDBName := dbName
		// Truncate the name if the length violates the allowed limit
		// As the passed dbName is same as chain/channel name, truncate using chainNameAllowedLength
		dbName = dbName[:chainNameAllowedLength]
		// For metadataDB (i.e., chain/channel DB), the dbName contains <first 50 chars
		// (i.e., chainNameAllowedLength) of chainName> + (SHA256 hash of actual chainName)
		dbName = dbName + "(" + hex.EncodeToString(util.ComputeSHA256([]byte(untruncatedDBName))) + ")"
		// 50 chars for dbName + 1 char for ( + 64 chars for sha256 + 1 char for ) = 116 chars
	}
	return dbName + "_"
}

// createCouchbaseDatabase creates a Couchbase database object, as well as the underlying database if it does not exist
func createCouchbaseDatabase(couchInstance *couchbaseInstance, dbName string) (*couchbaseDatabase, error) {
	databaseName, err := mapAndValidateDatabaseName(dbName)
	if err != nil {
		couchbaseLogger.Errorf("Error calling Couchbase CreateDatabaseIfNotExist() for dbName: %s, error: %s", dbName, err)
		return nil, err
	}

	couchDBDatabase := couchbaseDatabase{couchbaseInstance: couchInstance, dbName: databaseName}

	// Create CouchDB database upon ledger startup, if it doesn't already exist
	err = couchDBDatabase.createDatabaseIfNotExist()
	if err != nil {
		couchbaseLogger.Errorf("Error calling Couchbase CreateDatabaseIfNotExist() for dbName: %s, error: %s", dbName, err)
		return nil, err
	}

	return &couchDBDatabase, nil
}

func mapAndValidateDatabaseName(databaseName string) (string, error) {
	// test Length
	if len(databaseName) <= 0 {
		return "", errors.Errorf("database name is illegal, cannot be empty")
	}
	if len(databaseName) > maxLength {
		return "", errors.Errorf("database name is illegal, cannot be longer than %d", maxLength)
	}
	re, err := regexp.Compile(expectedDatabaseNamePattern)
	if err != nil {
		return "", errors.Wrapf(err, "error compiling regexp: %s", expectedDatabaseNamePattern)
	}
	matched := re.FindString(databaseName)
	if len(matched) != len(databaseName) {
		return "", errors.Errorf("databaseName '%s' does not match pattern '%s'", databaseName, expectedDatabaseNamePattern)
	}
	// replace all '.' to '%. The databaseName passed in will never contain an '%'.
	// So, this translation will not cause collisions
	databaseName = strings.Replace(databaseName, ".", "%", -1)
	return databaseName, nil
}

// The passed namespace will be in one of the following formats:
// <chaincode>                 - for namespaces containing regular public data
// <chaincode>%%p<collection>  - for namespaces containing private data collections
// <chaincode>%%h<collection>  - for namespaces containing hashes of private data collections
func constructNamespaceDBName(chainName, namespace string) string {
	chainName = strings.Replace(chainName, "$", "%", -1)
	namespace = strings.Replace(namespace, "$", "%", -1)
	namespaceDBName := chainName + "_" + namespace

	// For namespaceDBName of form 'chainName_namespace', on length limit violation, the truncated
	// namespaceDBName would contain <first 50 chars (i.e., chainNameAllowedLength) of chainName> + "_" +
	// <first 50 chars (i.e., namespaceNameAllowedLength) chars of namespace> +
	// (<SHA256 hash of [chainName_namespace]>)
	//
	// For namespaceDBName of form 'chainName_namespace%%[hp]collection', on length limit violation, the truncated
	// namespaceDBName would contain <first 50 chars (i.e., chainNameAllowedLength) of chainName> + "_" +
	// <first 50 chars (i.e., namespaceNameAllowedLength) of namespace> + "%%" + <first 50 chars
	// (i.e., collectionNameAllowedLength) of [hp]collection> + (<SHA256 hash of [chainName_namespace%%[hp]collection]>)

	if len(namespaceDBName) > maxLength {
		// Compute the hash of untruncated namespaceDBName that needs to be appended to
		// truncated namespaceDBName for maintaining uniqueness
		hashOfNamespaceDBName := hex.EncodeToString(util.ComputeSHA256([]byte(chainName + "_" + namespace)))

		// As truncated namespaceDBName is of form 'chainName_escapedNamespace', both chainName
		// and escapedNamespace need to be truncated to defined allowed length.
		if len(chainName) > chainNameAllowedLength {
			// Truncate chainName to chainNameAllowedLength
			chainName = chainName[0:chainNameAllowedLength]
		}
		// As escapedNamespace can be of either 'namespace' or 'namespace%%collectionName',
		// both 'namespace' and 'collectionName' need to be truncated to defined allowed length.
		// '%%' is used as joiner between namespace and collection name.
		// Split the escapedNamespace into escaped namespace and escaped collection name if exist.
		names := strings.Split(namespace, "%%")
		namespace := names[0]
		if len(namespace) > namespaceNameAllowedLength {
			// Truncate the namespace
			namespace = namespace[0:namespaceNameAllowedLength]
		}

		// Check and truncate the length of collection name if exist
		if len(names) == 2 {
			collection := names[1]
			if len(collection) > collectionNameAllowedLength {
				// Truncate the escaped collection name
				collection = collection[0:collectionNameAllowedLength]
			}
			// Append truncated collection name to escapedNamespace
			namespace = namespace + "%%" + collection
		}
		// Construct and return the namespaceDBName
		// 50 chars for chainName + 1 char for '_' + 102 chars for escaped namespace + 1 char for '(' + 64 chars
		// for sha256 hash + 1 char for ')' = 219 chars
		return chainName + "_" + namespace + "(" + hashOfNamespaceDBName + ")"
	}
	return namespaceDBName
}

// internalQueryLimit returns the maximum number of records to return internally
// when querying CouchDB.
//func (couchbaseInstance *couchbaseInstance) internalQueryLimit() int32 {
//	//return int32(couchInstance.conf.InternalQueryLimit)
//	return 20
//}

func dropDB(couchbaseInstance *couchbaseInstance, dbName string) error {
	db := &couchbaseDatabase{
		couchbaseInstance: couchbaseInstance,
		dbName:            dbName,
	}
	return db.dropDatabase()
}

func isCouchbaseInternalKey(key string) bool {
	return len(key) != 0 && key[0] == '_'
}

// printDocumentIds is a convenience method to print readable log entries for arrays of pointers
// to couch document IDs
func printDocumentIds(documentPointers []*couchbaseDoc) (string, error) {
	logger.Infof("Entering printDocumentIds() with %d documents", len(documentPointers))
	documentIds := []string{}

	for _, documentPointer := range documentPointers {
		docMetadata := &docMetadata{}
		docMetadata.ID = (*documentPointer)[idField].(string)
		docMetadata.Version = (*documentPointer)[versionField].(string)
		documentIds = append(documentIds, docMetadata.ID)
	}
	result := strings.Join(documentIds, ",")
	logger.Infof("Exiting printDocumentIds() with document IDs: %s", result)
	return result, nil
}
