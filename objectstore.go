package objectstore

import (
	"net/url"
)

type Object interface {
	Read() ([]byte, error)
	Write([]byte) error
	Delete() error
}

func New(path string) (Object, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	switch uri.Scheme {
	case "s3":
		return newS3Object(path), nil
	case "blob":
		return newAzureBlob(path), nil
	default:
		return newLocalFile(path), nil
	}
}
