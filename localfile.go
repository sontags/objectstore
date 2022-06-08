package objectstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type localFile struct {
	path string
}

func newLocalFile(path string) *localFile {
	return &localFile{path: path}
}

func (lf *localFile) Read() ([]byte, error) {
	data, err := ioutil.ReadFile(lf.path)
	if err != nil {
		return []byte{}, err
	}
	return data, nil
}

func (lf *localFile) Write(data []byte) error {
	dir := filepath.Dir(lf.path)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return fmt.Errorf("'%s' does not exist or is not a directory", dir)
	}

	err := ioutil.WriteFile(lf.path, data, 0640)
	if err != nil {
		return err
	}
	return nil
}

func (lf *localFile) Delete() error {
	err := os.Remove(lf.path)
	if err != nil {
		return err
	}
	return nil
}
