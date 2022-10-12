package objectstore

import (
	"fmt"
	"io"
	"net/http"
)

type httpFile struct {
	basepath string
}

func newHTTPFile(basepath string) *httpFile {
	return &httpFile{basepath: basepath}
}

func (hf *httpFile) Read(name string) ([]byte, error) {
	object := joinPath(hf.basepath, name)
	res, err := http.Get(object)
	if err != nil {
		return []byte{}, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}

func (hf *httpFile) Write(name string, data []byte) error {
	return fmt.Errorf("HTTP write is not implemented")
}

func (hf *httpFile) List() ([]string, error) {
	return []string{}, fmt.Errorf("HTTP list is not implemented")
}

func (hf *httpFile) Delete(name string) error {
	return fmt.Errorf("HTTP delete is not implemented")
}
