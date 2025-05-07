package statecouchbase

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/pkg/errors"
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

func stringToInt32(s string) (int32, error) {
	i64, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i64), nil
}

func int32ToString(i int32) string {
	return strconv.Itoa(int(i))
}

func populateQuery(query string, queryLimit int32, bookmark string, dbclient *couchbaseDatabase) (string, string, error) {

	couchbaseLogger.Debugf("Entering populateQuery() bookmark=[%q]", bookmark)
	updatedBookmark := int32(-1)
	query, _ = strings.CutSuffix(query, ";")
	qtemplate := template.New("SQL++")
	qtemplate, err := qtemplate.Parse(query)
	if err != nil {
		return "", "", err
	}

	var buf bytes.Buffer
	err = qtemplate.Execute(&buf, populator{Source: fmt.Sprintf("`%s`.`%s`.`%s`", dbclient.couchbaseInstance.conf.Bucket, dbclient.couchbaseInstance.conf.Scope, dbclient.dbName)})
	if err != nil {
		return "", "", err
	}
	query = buf.String()

	if queryLimit != 0 {
		query += fmt.Sprintf(" LIMIT %d", queryLimit)
	}

	if bookmark != "" {
		bookmarkInt, err := stringToInt32(bookmark)
		updatedBookmark = bookmarkInt + queryLimit
		if err != nil {
			return "", "", errors.Wrap(err, "Invalid bookmark")
		}
		query += fmt.Sprintf(" OFFSET %d", bookmarkInt)

	} else {
		updatedBookmark = queryLimit
	}
	couchbaseLogger.Debugf("Exiting populateQuery() bookmark=[%v]", updatedBookmark)
	return query, int32ToString(updatedBookmark), nil
}
