package statecouchbase

import (
	"strings"
)

type populator struct {
	Source string
}

func splitFromEnd(s string) (string, string) {
	idx := strings.LastIndex(s, ";")
	if idx == -1 {
		return s, "" // no semicolon found
	}
	return s[:idx], s[idx+1:]
}

func populateQuery(query string, queryLimit int32, bookmark string) (string, error) {
	//	query, _ = strings.CutSuffix(query, ";")
	//	qtemplate := template.New("SQL++")
	//	qtemplate, err := qtemplate.Parse(query)
	//	if err != nil {
	//		return "", err
	//	}
	//
	//	var buf bytes.Buffer
	//	err = qtemplate.Execute(&buf, populator{Source: fmt.Sprintf("`%s`.`%s`.`%s`", dbclient.couchbaseInstance.conf.Bucket, dbclient.couchbaseInstance.conf.Scope, dbclient.dbName)})
	//	if err != nil {
	//		return "", err
	//	}
	//	query = buf.String()
	//
	//	if bookmark == "" {
	//
	//	} else {
	//		cb_bookmark, err := strconv.Atoi(bookmark)
	//		if err != nil {
	//			return "", err
	//		}
	//	}
	return "", nil
}
