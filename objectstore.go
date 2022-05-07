package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type backendType int

const (
	s3Object backendType = iota
	blobObject
	fileObject
)

type Object struct {
	path    string
	backend backendType
}

func New(path string) (Object, error) {
	o := Object{path: path}

	uri, err := url.Parse(path)
	if err != nil {
		return o, err
	}

	switch uri.Scheme {
	case "s3":
		o.backend = s3Object
	case "blob":
		o.backend = blobObject
	default:
		o.backend = fileObject
	}
	return o, nil
}

func (o Object) Read() ([]byte, error) {
	switch o.backend {
	case s3Object:
		bucket, object, err := parseS3Path(o.path)
		if err != nil {
			return []byte{}, err
		}

		s3svc := s3.New(session.New())
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}

		result, err := s3svc.GetObject(input)
		if err != nil {
			return []byte{}, err
		}

		buf := new(bytes.Buffer)
		buf.ReadFrom(result.Body)

		data := []byte{}
		file := buf.Bytes()
		if len(file) > 0 {
			data = append(data, file...)
		}
		return data, nil
	case blobObject:
		account, container, object, err := parseBlobPath(o.path)
		if err != nil {
			return []byte{}, err
		}

		blobClient, err := getBlobClient(account, container, object)
		if err != nil {
			return []byte{}, err
		}

		ctx := context.Background()

		resp, err := blobClient.Download(ctx, nil)
		if err != nil {
			return []byte{}, err
		}

		reader := resp.Body(nil)
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return []byte{}, err
		}
		return data, nil
	default:
		data, err := ioutil.ReadFile(o.path)
		if err != nil {
			return []byte{}, err
		}
		return data, nil
	}
}

func (o Object) Write(data []byte) error {
	switch o.backend {
	case s3Object:
		bucket, object, err := parseS3Path(o.path)
		if err != nil {
			return err
		}

		s3svc := s3.New(session.New())
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
			ACL:    aws.String("private"),
			Body:   bytes.NewReader(data),
			//ContentLength: aws.Int64(len(data)),
			ContentType: aws.String(http.DetectContentType(data)),
		}

		_, err = s3svc.PutObject(input)
		if err != nil {
			return err
		}
	case blobObject:
		account, container, object, err := parseBlobPath(o.path)
		if err != nil {
			return err
		}

		blobClient, err := getBlobClient(account, container, object)
		if err != nil {
			return err
		}
		ctx := context.Background()

		_, err = blobClient.UploadBuffer(ctx, data, azblob.UploadOption{})
		if err != nil {
			return err
		}
	default:
		dir := filepath.Dir(o.path)
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			return fmt.Errorf("'%s' does not exist or is not a directory", dir)
		}

		err := ioutil.WriteFile(o.path, data, 0640)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o Object) Delete() error {
	switch o.backend {
	case s3Object:
		bucket, object, err := parseS3Path(o.path)
		if err != nil {
			return err
		}
		s3svc := s3.New(session.New())
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}

		_, err = s3svc.DeleteObject(input)
		if err != nil {
			return err
		}
	case blobObject:
		account, container, object, err := parseBlobPath(o.path)
		if err != nil {
			return err
		}

		blobClient, err := getBlobClient(account, container, object)
		if err != nil {
			return err
		}
		ctx := context.Background()

		_, err = blobClient.Delete(ctx, nil)
		if err != nil {
			return err
		}
	default:
		err := os.Remove(o.path)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
// Used DefaultAzureCredentials, does not work
func getBlobClient(account, container, object string) (*azblob.BlockBlobClient, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}

	accountPath := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	serviceClient, err := azblob.NewServiceClient(accountPath, credential, nil)
	if err != nil {
		return nil, err
	}

	sasURL, err := serviceClient.GetSASURL(
		azblob.AccountSASResourceTypes{Object: true, Service: true, Container: true},
		azblob.AccountSASPermissions{Read: true, List: true},
		time.Now(),
		time.Now().Add(48*time.Hour),
	)
	if err != nil {
		return nil, err
	}

	blobClient, err := azblob.NewBlockBlobClientWithNoCredential(sasURL, nil)
	if err != nil {
		return nil, err
	}

	return blobClient, nil
}
*/

func getBlobClient(account, container, object string) (*azblob.BlockBlobClient, error) {
	accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
	if !ok {
		return nil, errors.New("AZURE_STORAGE_ACCOUNT_KEY could not be found")
	}

	credential, err := azblob.NewSharedKeyCredential(account, accountKey)
	if err != nil {
		return nil, err
	}

	accountPath := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	serviceClient, err := azblob.NewServiceClientWithSharedKey(accountPath, credential, nil)
	if err != nil {
		return nil, err
	}

	containerClient, err := serviceClient.NewContainerClient(container)
	if err != nil {
		return nil, err
	}

	blobClient, err := containerClient.NewBlockBlobClient(object)
	if err != nil {
		return nil, err
	}

	return blobClient, nil
}

func parseBlobPath(path string) (account, container, object string, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return
	}
	fragments := strings.SplitN(strings.TrimPrefix(uri.Path, "/"), "/", 2)
	if len(fragments) != 2 {
		err = fmt.Errorf("unusable path '%s'", uri.Path)
		return
	}

	account = uri.Host
	container = strings.Trim(fragments[0], "/")
	object = fragments[1]
	return
}

func parseS3Path(path string) (bucket, object string, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return
	}
	bucket = uri.Host
	object = uri.Path
	return
}
