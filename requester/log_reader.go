package requester

import (
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"sync"
)

// QueryParamProvider dynamically produces HTTP Query parameters
type QueryParamProvider interface {
	Parameters(queryParams url.Values) (url.Values, error)
	Close()
}

// QueryParamCSVProvider provides HTTP Query Parameters from a CSV file
type QueryParamCSVProvider struct {
	file      *os.File
	csvReader *csv.Reader
	readLock  sync.Mutex
	header    []string
}

// InitQueryParamCSVProvider creates and returns a QueryParamCSVProvider
func InitQueryParamCSVProvider(queryParamCSVPath string) (QueryParamProvider, error) {
	if queryParamCSVPath == "" {
		return nil, nil
	}
	file, err := os.Open(queryParamCSVPath)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	return &QueryParamCSVProvider{
		file:      file,
		csvReader: reader,
		header:    header,
	}, nil
}

// Parameters provides synchronized access to the next set of query parameters from the CSV reader
func (qp *QueryParamCSVProvider) Parameters(queryParams url.Values) (url.Values, error) {
	qp.readLock.Lock()
	values, err := qp.csvReader.Read()
	if err != nil {
		return nil, err
	}
	qp.readLock.Unlock()

	if len(values) != len(qp.header) {
		return nil, fmt.Errorf("Malformed CSV line found, not enough or too few values: %v", values)
	}

	for idx, header := range qp.header {
		queryParams.Add(header, values[idx])
	}
	return queryParams, nil
}

// Close closes the open CSV file
func (qp *QueryParamCSVProvider) Close() {
	qp.file.Close()
}
