package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type azureBlob struct {
	path string
}

func newAzureBlob(path string) *azureBlob {
	return &azureBlob{path: path}
}

func (ab *azureBlob) Read() ([]byte, error) {
	account, container, object, err := parseBlobPath(ab.path)
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
}

func (ab *azureBlob) Write(data []byte) error {
	account, container, object, err := parseBlobPath(ab.path)
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
	return nil
}

func (ab *azureBlob) Delete() error {
	account, container, object, err := parseBlobPath(ab.path)
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
	return nil
}

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
