package objectstore

import (
	"fmt"
	"net/url"
	"strings"
)

type Object interface {
	Read(string) ([]byte, error)
	Write(string, []byte) error
	Delete(string) error
	List() ([]string, error)
}

func New(basepath string) (Object, error) {
	uri, err := url.Parse(basepath)
	if err != nil {
		return nil, err
	}

	switch uri.Scheme {
	case "s3":
		return newS3Object(basepath), nil
	case "blob":
		return newAzureBlob(basepath), nil
	case "http", "https":
		return newHTTPFile(basepath), nil
	default:
		return newLocalFile(basepath), nil
	}
}

func joinPath(basepath, name string) string {
	basepath = strings.TrimRight(basepath, "/")
	name = strings.TrimLeft(name, "/")
	return fmt.Sprintf("%s/%s", basepath, name)
}
