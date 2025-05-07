/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecouchbase

import (
	_ "bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
)

const (
	attachmentField = "_attachment"
	idField         = "_id"
	versionField    = "~version"
	deletedField    = "_deleted"
)

type keyValue struct {
	key string
	*statedb.VersionedValue
}

//type jsonValue map[string]interface{}

func tryCastingToJSON(b []byte) (isJSON bool, val couchbaseDoc) {
	var couchbaseDocument couchbaseDoc
	err := json.Unmarshal(b, &couchbaseDocument)
	return err == nil, couchbaseDocument
}

//func castToJSON(b []byte) (jsonValue, error) {
//	var jsonVal map[string]interface{}
//	err := json.Unmarshal(b, &jsonVal)
//	err = errors.Wrap(err, "error unmarshalling json data")
//	return jsonVal, err
//}

func (c *couchbaseDoc) checkReservedFieldsNotPresent() error {
	for fieldName := range *c {
		if fieldName == versionField || strings.HasPrefix(fieldName, "_") {
			return errors.Errorf("field [%s] is not valid for the CouchDB state database", fieldName)
		}
	}
	return nil
}

func (c *couchbaseDoc) toBytes() ([]byte, error) {
	jsonBytes, err := json.Marshal(c)
	err = errors.Wrap(err, "error marshalling json data")
	return jsonBytes, err
}

func (c *couchbaseDoc) key() (string, error) {
	logger.Debugf("Entering couchbaseDoc.key()")
	key := (*c)[idField].(string)
	logger.Debugf("Exiting couchbaseDoc.key() with key: %s", key)
	return key, nil
}

func couchbaseDocToKeyValue(doc *couchbaseDoc) (*keyValue, error) {
	docFields, err := validateAndRetrieveFields(doc)
	if err != nil {
		return nil, err
	}
	_version, metadata, err := decodeVersionAndMetadata(docFields.versionAndMetadata)
	if err != nil {
		return nil, err
	}
	return &keyValue{
		docFields.id,
		&statedb.VersionedValue{
			Value:    docFields.value,
			Version:  _version,
			Metadata: metadata,
		},
	}, nil
}

type couchbaseDocFields struct {
	id                 string
	revision           string
	value              []byte
	versionAndMetadata string
}

func validateAndRetrieveFields(doc *couchbaseDoc) (*couchbaseDocFields, error) {
	couchbaseLogger.Debugf("Entering validateAndRetrieveFields, doc: %v", doc)
	//decoder := json.NewDecoder(bytes.NewBuffer(*doc))
	//decoder.UseNumber()
	//if err := decoder.Decode(&jsonDoc); err != nil {
	//	return nil, err
	//}
	//logger.Inffo()("Couchbase doc fields: %s", jsonDoc)
	docFields := &couchbaseDocFields{}

	if (*doc)[idField] != nil {
		docFields.id = (*doc)[idField].(string)
		delete(*doc, idField)
	}

	if (*doc)[versionField] == nil {
		return nil, fmt.Errorf("version field %s was not found", versionField)
	}
	docFields.versionAndMetadata = (*doc)[versionField].(string)

	delete(*doc, versionField)

	var err error

	if (*doc)[attachmentField] == nil {
		logger.Debugf("Attachment field is missing, just sending back the stupid stuff")
		docFields.value, err = doc.toBytes()
		return docFields, err
	}
	attachmentB64 := (*doc)[attachmentField].(string)
	logger.Debugf("Couchbase doc attachements (After marshalling): %v", attachmentB64)

	docFields.value, err = b64.StdEncoding.DecodeString(attachmentB64)
	logger.Debugf("Couchbase doc attachements (Before marshalling): %v", docFields.value)

	//docFields.value, err = json.Marshal(attachmentString)
	//logger.Debugf()("Couchbase doc attachements (After marshalling): %v", docFields)
	delete(*doc, attachmentField)
	return docFields, err
}

func keyValToCouchbaseDoc(kv *keyValue) (*couchbaseDoc, error) {
	type kvType int32
	const (
		kvTypeDelete = iota
		kvTypeJSON
		kvTypeAttachment
	)
	key, value, metadata, _version := kv.key, kv.Value, kv.Metadata, kv.Version
	couchbaseDocument := make(couchbaseDoc)

	var kvtype kvType
	switch {
	case value == nil:
		kvtype = kvTypeDelete
	// check for the case where the jsonMap is nil,  this will indicate
	// a special case for the Unmarshal that results in a valid JSON returning nil
	case json.Unmarshal(value, &couchbaseDocument) == nil && couchbaseDocument != nil:
		kvtype = kvTypeJSON
		if err := couchbaseDocument.checkReservedFieldsNotPresent(); err != nil {
			return nil, err
		}
	default:
		// create an empty map, if the map is nil
		if couchbaseDocument == nil {
			couchbaseDocument = make(couchbaseDoc)
		}
		kvtype = kvTypeAttachment
	}

	verAndMetadata, err := encodeVersionAndMetadata(_version, metadata)
	if err != nil {
		return nil, err
	}
	// add the (version + metadata), id, revision, and delete marker (if needed)
	couchbaseDocument[versionField] = verAndMetadata
	couchbaseDocument[idField] = key
	if kvtype == kvTypeDelete {
		couchbaseDocument[deletedField] = true
	}
	if kvtype == kvTypeAttachment {
		couchbaseDocument[attachmentField] = value
	}
	return &couchbaseDocument, nil
}

// couchSavepointData data for couchdb
type couchbaseSavepointData struct {
	BlockNum uint64 `json:"BlockNum"`
	TxNum    uint64 `json:"TxNum"`
}

type channelMetadata struct {
	Id          string `json:"_id"`
	ChannelName string `json:"ChannelName"`
	// namespace to namespaceDBInfo mapping
	NamespaceDBsInfo map[string]*namespaceDBInfo `json:"NamespaceDBsInfo"`
}

type namespaceDBInfo struct {
	Namespace string `json:"Namespace"`
	DBName    string `json:"DBName"`
}

func encodeSavepoint(height *version.Height) (*couchbaseDoc, error) {
	var couchbaseDoc = make(couchbaseDoc)
	// construct savepoint document
	couchbaseDoc["BlockNum"] = height.BlockNum
	couchbaseDoc["TxNum"] = height.TxNum
	return &couchbaseDoc, nil
}

func decodeSavepoint(couchbaseDoc *couchbaseDoc) (*version.Height, error) {
	savepointDoc := &couchbaseSavepointData{}
	savepointDoc.TxNum = uint64((*couchbaseDoc)["TxNum"].(float64))
	return &version.Height{BlockNum: savepointDoc.BlockNum, TxNum: savepointDoc.TxNum}, nil
}

//func encodeChannelMetadata(metadataDoc *channelMetadata) (*couchbaseDoc, error) {
//	metadataJSON, err := json.Marshal(metadataDoc)
//	if err != nil {
//		err = errors.Wrap(err, "failed to marshal channel metadata")
//		logger.Errorf("%+v", err)
//		return nil, err
//	}
//	return &couchbaseDoc{jsonValue: metadataJSON}, nil
//}

func decodeChannelMetadata(couchbaseDoc *couchbaseDoc) (*channelMetadata, error) {
	couchbaseLogger.Debugf("Entering decodeChannelMetadata() with doc: %v", couchbaseDoc)

	channelName, ok := (*couchbaseDoc)["ChannelName"].(string)
	if !ok {
		return nil, fmt.Errorf("ChannelName is not a string")
	}

	rawNamespaceInfo, ok := (*couchbaseDoc)["NamespaceDBsInfo"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("NamespaceDBsInfo is not a map[string]interface{}")
	}

	namespaceInfo := make(map[string]*namespaceDBInfo)
	for k, v := range rawNamespaceInfo {
		valMap, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("value for key %s is not a map[string]interface{}", k)
		}

		ns := &namespaceDBInfo{}
		if namespace, ok := valMap["Namespace"].(string); ok {
			ns.Namespace = namespace
		}
		if dbName, ok := valMap["DBName"].(string); ok {
			ns.DBName = dbName
		}
		namespaceInfo[k] = ns
	}

	metadataDoc := &channelMetadata{
		ChannelName:      channelName,
		NamespaceDBsInfo: namespaceInfo,
	}

	couchbaseLogger.Debugf("Exiting decodeChannelMetadata() with metadatadoc: %v", metadataDoc)
	return metadataDoc, nil
}

type dataformatInfo struct {
	Version string `json:"Version"`
}

func encodeDataformatInfo(dataFormatVersion string) (*couchbaseDoc, error) {
	var couchbaseDoc = make(couchbaseDoc)

	//dataformatInfoJSON, err := json.Marshal(dataformatInfo)
	//if err != nil {
	//	err = errors.Wrapf(err, "failed to marshal dataformatInfo [%#v]", dataformatInfo)
	//	logger.Errorf("%+v", err)
	//	return nil, err
	//}
	couchbaseDoc["Version"] = dataFormatVersion

	return &couchbaseDoc, nil
}

func decodeDataformatInfo(couchbaseDoc *couchbaseDoc) (string, error) {
	dataformatInfo := &dataformatInfo{}
	dataformatInfo.Version = (*couchbaseDoc)["Version"].(string)
	return dataformatInfo.Version, nil
}

func validateValue(value []byte) error {
	isJSON, jsonVal := tryCastingToJSON(value)
	if !isJSON {
		return nil
	}
	return jsonVal.checkReservedFieldsNotPresent()
}

func validateKey(key string) error {
	if !utf8.ValidString(key) {
		return errors.Errorf("invalid key [%x], must be a UTF-8 string", key)
	}
	if strings.HasPrefix(key, "_") {
		return errors.Errorf("invalid key [%s], cannot begin with \"_\"", key)
	}
	if key == "" {
		return errors.New("invalid key. Empty string is not supported as a key by couchbase")
	}
	return nil
}

//// removeJSONRevision removes the "_rev" if this is a JSON
//func removeJSONRevision(jsonValue *[]byte) error {
//	jsonVal, err := castToJSON(*jsonValue)
//	if err != nil {
//		logger.Errorf("Failed to unmarshal couchdb JSON data: %+v", err)
//		return err
//	}
//	jsonVal.removeRevField()
//	if *jsonValue, err = jsonVal.toBytes(); err != nil {
//		logger.Errorf("Failed to marshal couchdb JSON data: %+v", err)
//	}
//	return err
//}
