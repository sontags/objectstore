package objectstore

import (
	"fmt"
	"os"
	"path/filepath"
)

type localFile struct {
	basepath string
}

func newLocalFile(basepath string) *localFile {
	return &localFile{basepath: basepath}
}

func (lf *localFile) Read(name string) ([]byte, error) {
	object := lf.joinPath(lf.basepath, name)
	return os.ReadFile(object)
}

func (lf *localFile) Write(name string, data []byte) error {
	object := lf.joinPath(lf.basepath, name)
	dir := filepath.Dir(object)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return fmt.Errorf("'%s' does not exist or is not a directory", dir)
	}
	return os.WriteFile(object, data, 0640)
}

func (lf *localFile) List() ([]string, error) {
	files := []string{}
	err := filepath.Walk(lf.basepath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, info.Name())
		}
		return nil
	})
	return files, err
}

func (lf *localFile) Delete(name string) error {
	object := lf.joinPath(lf.basepath, name)
	return os.Remove(object)
}

func (lf *localFile) joinPath(basepath, name string) string {
	return filepath.Join(basepath, name)
}
