package objectstore

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type httpFile struct {
	path string
}

func newHTTPFile(path string) *localFile {
	return &localFile{path: path}
}

func (hf *httpFile) Read() ([]byte, error) {
	res, err := http.Get(hf.path)
	if err != nil {
		return []byte{}, err
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return []byte{}, err
	}
	return data, nil
}

func (hf *httpFile) Write(data []byte) error {
	return fmt.Errorf("HTTP write is not implemented")
}

func (hf *httpFile) Delete() error {
	return fmt.Errorf("HTTP delete is not implemented")
}
